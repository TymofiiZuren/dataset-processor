package durable

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/logging"
)

// NewAWSObjects uses the SDK credential chain (including workload roles/SSO),
// never local-emulator credentials. Construction does not contact AWS.
func NewAWSObjects(ctx context.Context, c Config) (*Objects, error) {
	if e := c.ValidateAWS(); e != nil {
		return nil, e
	}
	cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(c.Region), config.WithRetryMaxAttempts(2), config.WithHTTPClient(awshttp.NewBuildableClient().WithTimeout(30*time.Second)), config.WithLogger(logging.NewStandardLogger(io.Discard)))
	if e != nil {
		return nil, errors.New("AWS configuration unavailable; check the workload credential provider")
	}
	return newAWSObjects(c, cfg)
}
func newAWSObjects(c Config, cfg aws.Config) (*Objects, error) {
	if e := c.ValidateAWS(); e != nil {
		return nil, e
	}
	cfg.Region = c.Region
	cfg.RetryMaxAttempts = 2
	cfg.Logger = logging.NewStandardLogger(io.Discard)
	// Explicit standard endpoints override shared-config/emulator endpoint settings.
	// Keep the SDK S3 resolver so it can apply standard virtual-host addressing.
	o := &Objects{Bucket: c.Bucket, Queue: c.Queue, Cloud: true, Backend: c.Backend(), Region: c.Region, Account: c.Account}
	o.S3 = s3.NewFromConfig(cfg, func(v *s3.Options) {
		v.BaseEndpoint = aws.String("https://s3." + c.Region + ".amazonaws.com")
		v.UsePathStyle = false
		v.UseAccelerate = false
		v.UseARNRegion = false
	})
	o.SQS = sqs.NewFromConfig(cfg, func(v *sqs.Options) { v.BaseEndpoint = aws.String("https://sqs." + c.Region + ".amazonaws.com") })
	return o, nil
}
func (o *Objects) expectedOwner() *string {
	if o.Cloud {
		return &o.Account
	}
	return nil
}
func (o *Objects) checkAWS(ctx context.Context) error {
	owner := o.expectedOwner()
	h, e := o.S3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &o.Bucket, ExpectedBucketOwner: owner})
	if e != nil || aws.ToString(h.BucketRegion) != o.Region {
		return errors.New("AWS bucket unavailable or account/region mismatch; check bucket ownership and workload permissions")
	}
	version, e := o.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: &o.Bucket, ExpectedBucketOwner: owner})
	if e != nil || version.Status != types.BucketVersioningStatusEnabled {
		return errors.New("AWS bucket must have versioning enabled")
	}
	public, e := o.S3.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: &o.Bucket, ExpectedBucketOwner: owner})
	if e != nil || public.PublicAccessBlockConfiguration == nil {
		return errors.New("AWS bucket public access protections unavailable")
	}
	p := public.PublicAccessBlockConfiguration
	if !aws.ToBool(p.BlockPublicAcls) || !aws.ToBool(p.IgnorePublicAcls) || !aws.ToBool(p.BlockPublicPolicy) || !aws.ToBool(p.RestrictPublicBuckets) {
		return errors.New("AWS bucket requires all four public access blocks")
	}
	enc, e := o.S3.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: &o.Bucket, ExpectedBucketOwner: owner})
	if e != nil || enc.ServerSideEncryptionConfiguration == nil || len(enc.ServerSideEncryptionConfiguration.Rules) != 1 || enc.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault == nil || enc.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != types.ServerSideEncryptionAes256 {
		return errors.New("AWS profile requires bucket SSE-S3 encryption (AES256)")
	}
	ownership, e := o.S3.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: &o.Bucket, ExpectedBucketOwner: owner})
	if e != nil || ownership.OwnershipControls == nil || len(ownership.OwnershipControls.Rules) != 1 || ownership.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerEnforced {
		return errors.New("AWS bucket must enforce bucket-owner object ownership")
	}
	lifecycle, e := o.S3.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: &o.Bucket, ExpectedBucketOwner: owner})
	if e != nil {
		var api interface{ ErrorCode() string }
		if !errors.As(e, &api) || api.ErrorCode() != "NoSuchLifecycleConfiguration" {
			return errors.New("AWS bucket lifecycle configuration unavailable")
		}
	} else {
		for _, rule := range lifecycle.Rules {
			if rule.Status == types.ExpirationStatusEnabled && (rule.Expiration != nil || rule.NoncurrentVersionExpiration != nil) {
				return errors.New("AWS bucket expiration can delete referenced versions; remove expiration rules before use")
			}
		}
	}
	attrs, e := o.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &o.Queue, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn, sqstypes.QueueAttributeNameSqsManagedSseEnabled, sqstypes.QueueAttributeNameVisibilityTimeout, sqstypes.QueueAttributeNameRedrivePolicy}})
	if e != nil {
		return errors.New("AWS queue attributes unavailable; check workload permissions")
	}
	u, _ := url.Parse(o.Queue)
	name := u.Path[strings.LastIndex(u.Path, "/")+1:]
	expected := "arn:aws:sqs:" + o.Region + ":" + o.Account + ":" + name
	visibility, err := strconv.Atoi(attrs.Attributes["VisibilityTimeout"])
	if attrs.Attributes["QueueArn"] != expected || attrs.Attributes["SqsManagedSseEnabled"] != "true" || err != nil || visibility < 90 {
		return errors.New("AWS queue must match configured identity, enable SSE-SQS and use visibility of at least 90 seconds")
	}
	var redrive struct {
		Target string      `json:"deadLetterTargetArn"`
		Max    json.Number `json:"maxReceiveCount"`
	}
	if e = json.Unmarshal([]byte(attrs.Attributes["RedrivePolicy"]), &redrive); e != nil {
		return errors.New("AWS queue requires a dead-letter redrive policy")
	}
	n, e := strconv.Atoi(string(redrive.Max))
	prefix := "arn:aws:sqs:" + o.Region + ":" + o.Account + ":"
	if e != nil || n < 5 || n > 20 || !strings.HasPrefix(redrive.Target, prefix) || redrive.Target == expected || strings.Contains(strings.TrimPrefix(redrive.Target, prefix), ":") {
		return errors.New("AWS dead-letter policy must target a different standard queue in this account/region with 5–20 receives")
	}
	dlqName := strings.TrimPrefix(redrive.Target, prefix)
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`).MatchString(dlqName) {
		return errors.New("AWS dead-letter queue name is unsupported")
	}
	dlqURL := "https://sqs." + o.Region + ".amazonaws.com/" + o.Account + "/" + dlqName
	dlq, e := o.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &dlqURL, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn, sqstypes.QueueAttributeNameSqsManagedSseEnabled}})
	if e != nil || dlq.Attributes["QueueArn"] != redrive.Target || dlq.Attributes["SqsManagedSseEnabled"] != "true" {
		return errors.New("AWS dead-letter queue unavailable or missing SSE-SQS encryption")
	}
	return nil
}

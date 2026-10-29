package durable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go/logging"
)

func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type Objects struct {
	Backend, Endpoint string
	S3                *s3.Client
	SQS               *sqs.Client
	Bucket, Queue     string
	Cloud             bool
	Cluster           bool
	Region, Account   string
}

func NewObjects(ctx context.Context, endpoint, bucket, queueName string) (*Objects, error) {
	u, e := url.Parse(endpoint)
	if e != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Path != "" && u.Path != "/" {
		return nil, errors.New("local endpoint must be plain loopback HTTP")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("local endpoint must be loopback; AWS fallback refused")
	}
	return emulatorObjects(ctx, endpoint, bucket, queueName, false)
}
func NewClusterObjects(ctx context.Context, endpoint, bucket, queueName string) (*Objects, error) {
	if endpoint != ClusterEndpoint {
		return nil, errors.New("cluster emulator endpoint is fixed")
	}
	return emulatorObjects(ctx, endpoint, bucket, queueName, true)
}
func emulatorObjects(ctx context.Context, endpoint, bucket, queueName string, cluster bool) (*Objects, error) {
	cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local-emulator", "local-emulator", "")), config.WithRetryMaxAttempts(2), config.WithLogger(logging.NewStandardLogger(io.Discard)))
	if e != nil {
		return nil, safeError("local SDK")
	}
	o := &Objects{Cluster: cluster, Bucket: bucket, Endpoint: endpoint, Backend: "local:" + endpoint + ":" + bucket + ":" + queueName, S3: s3.NewFromConfig(cfg, func(v *s3.Options) { v.BaseEndpoint = aws.String(endpoint); v.UsePathStyle = true }), SQS: sqs.NewFromConfig(cfg, func(v *sqs.Options) { v.BaseEndpoint = aws.String(endpoint) })}
	q, e := o.SQS.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if e == nil {
		o.Queue = aws.ToString(q.QueueUrl)
	}
	return o, nil
}
func (o *Objects) Init(ctx context.Context, queueName string) error {
	if o.Cloud {
		return errors.New("AWS resources must be provisioned separately; local init refuses cloud changes")
	}
	if _, e := o.S3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &o.Bucket}); e != nil {
		if _, e = o.S3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &o.Bucket}); e != nil {
			return safeError("local bucket")
		}
	}
	if _, e := o.S3.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: &o.Bucket, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); e != nil {
		return safeError("bucket versioning")
	}
	dlq, e := o.SQS.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName + "-dead")})
	if e != nil {
		return safeError("dead letter queue")
	}
	attr, e := o.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: dlq.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if e != nil {
		return safeError("dead letter queue")
	}
	policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attr.Attributes["QueueArn"], "maxReceiveCount": "20"})
	q, e := o.SQS.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName, Attributes: map[string]string{"VisibilityTimeout": "90", "RedrivePolicy": string(policy)}})
	if e != nil {
		return safeError("local queue")
	}
	o.Queue = aws.ToString(q.QueueUrl)
	o.Backend = "local:" + o.Endpoint + ":" + o.Bucket + ":" + queueName
	return o.Check(ctx)
}
func (o *Objects) Check(ctx context.Context) error {
	if o.Cloud {
		return o.checkAWS(ctx)
	}
	v, e := o.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: &o.Bucket})
	if e != nil || v.Status != types.BucketVersioningStatusEnabled {
		return errors.New("versioned local bucket unavailable; run local init")
	}
	if o.Queue == "" {
		return errors.New("local queue unavailable; run local init")
	}
	return nil
}
func (o *Objects) Put(ctx context.Context, key string, b []byte) (Ref, error) {
	h := Hash(b)
	v, e := o.S3.PutObject(ctx, &s3.PutObjectInput{ExpectedBucketOwner: o.expectedOwner(), Bucket: &o.Bucket, Key: &key, Body: bytes.NewReader(b), Metadata: map[string]string{"sha256": h}, ServerSideEncryption: types.ServerSideEncryptionAes256})
	if e != nil {
		return Ref{}, safeError("object write")
	}
	version := aws.ToString(v.VersionId)
	if version == "" || version == "null" {
		return Ref{}, ErrIntegrity
	}
	return Ref{key, version, h, int64(len(b))}, nil
}

// Read checks the entire exact version before callers transform or expose it.
// Objects are bounded by the 8 MiB upload / 64 MiB derived reservation limits.
func (o *Objects) Read(ctx context.Context, r Ref) ([]byte, error) {
	if r.Version == "" || r.Version == "null" || r.Size < 0 || r.Size > maxDerived {
		return nil, ErrIntegrity
	}
	v, e := o.S3.GetObject(ctx, &s3.GetObjectInput{ExpectedBucketOwner: o.expectedOwner(), Bucket: &o.Bucket, Key: &r.Key, VersionId: &r.Version})
	if e != nil {
		var missing *types.NoSuchKey
		var api interface{ ErrorCode() string }
		if errors.As(e, &missing) || errors.As(e, &api) && (api.ErrorCode() == "NoSuchVersion" || api.ErrorCode() == "NoSuchKey") {
			return nil, ErrIntegrity
		}
		return nil, safeError("object read")
	}
	defer v.Body.Close()
	b, e := io.ReadAll(io.LimitReader(v.Body, r.Size+1))
	if e != nil {
		return nil, safeError("object read")
	}
	if int64(len(b)) != r.Size || Hash(b) != r.SHA256 {
		return nil, ErrIntegrity
	}
	return b, nil
}

// RecoverInput only examines the job's unique intent key. Concurrent duplicate
// submissions share an idempotent content descriptor; selected versions stay pinned.
func (o *Objects) RecoverInput(ctx context.Context, key string) (Ref, error) {
	v, e := o.S3.HeadObject(ctx, &s3.HeadObjectInput{ExpectedBucketOwner: o.expectedOwner(), Bucket: &o.Bucket, Key: &key})
	if e != nil {
		return Ref{}, safeError("upload reconciliation")
	}
	r := Ref{key, aws.ToString(v.VersionId), v.Metadata["sha256"], aws.ToInt64(v.ContentLength)}
	if r.SHA256 == "" || r.Size > MaxInput {
		return Ref{}, ErrIntegrity
	}
	_, e = o.Read(ctx, r)
	return r, e
}
func (o *Objects) Send(ctx context.Context, d Dispatch) error {
	b, _ := json.Marshal(d)
	_, e := o.SQS.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &o.Queue, MessageBody: aws.String(string(b))})
	if e != nil {
		return safeError("queue send")
	}
	return nil
}
func (o *Objects) Receive(ctx context.Context) (*Message, error) {
	v, e := o.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &o.Queue, MaxNumberOfMessages: 1, WaitTimeSeconds: 2, VisibilityTimeout: 90})
	if e != nil {
		return nil, safeError("queue receive")
	}
	if len(v.Messages) == 0 {
		return nil, nil
	}
	m := v.Messages[0]
	var d Dispatch
	if e = json.Unmarshal([]byte(aws.ToString(m.Body)), &d); e != nil {
		return &Message{Receipt: aws.ToString(m.ReceiptHandle)}, nil
	}
	return &Message{Dispatch: d, Receipt: aws.ToString(m.ReceiptHandle)}, nil
}

type Message struct {
	Dispatch
	Receipt string
}

func (o *Objects) Ack(ctx context.Context, m Message) error {
	_, e := o.SQS.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &o.Queue, ReceiptHandle: &m.Receipt})
	if e != nil {
		return safeError("queue acknowledgement")
	}
	return nil
}
func (o *Objects) Visibility(ctx context.Context, m Message) error {
	_, e := o.SQS.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: &o.Queue, ReceiptHandle: &m.Receipt, VisibilityTimeout: int32((90 * time.Second).Seconds())})
	if e != nil {
		return safeError("queue visibility")
	}
	return nil
}

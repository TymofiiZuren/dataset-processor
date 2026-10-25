package durable

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func validAWSConfig() Config {
	return Config{Profile: "aws", Region: "eu-west-1", Account: "123456789012", Bucket: "processor-fixture", Queue: "https://sqs.eu-west-1.amazonaws.com/123456789012/processor-work", Database: "postgres://localhost/processor_aws?sslmode=disable"}
}
func TestAWSConfigurationRejectsAmbiguousTargets(t *testing.T) {
	for _, change := range []func(*Config){func(c *Config) { c.Account = "" }, func(c *Config) { c.Region = "" }, func(c *Config) { c.Bucket = "arn:aws:s3:::bucket" }, func(c *Config) { c.Endpoint = "http://localhost:5000" }, func(c *Config) { c.Queue = "https://sqs.us-east-1.amazonaws.com/123456789012/work" }, func(c *Config) { c.Queue = "https://sqs.eu-west-1.amazonaws.com/999999999999/work" }, func(c *Config) { c.Queue += "?redirect=elsewhere" }, func(c *Config) { c.Queue = "https://sqs.eu-west-1.amazonaws.com/123456789012/work.fifo" }, func(c *Config) { c.Database = "" }} {
		c := validAWSConfig()
		change(&c)
		if c.ValidateAWS() == nil {
			t.Fatal("ambiguous cloud target accepted")
		}
	}
	c := validAWSConfig()
	if e := c.ValidateAWS(); e != nil {
		t.Fatal(e)
	}
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://localhost:5000")
	if e := c.ValidateAWS(); e == nil {
		t.Fatal("emulator endpoint leaked into AWS profile")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }
func TestAWSAdapterPinsEndpointsAndExpectedOwnerWithoutProvisioning(t *testing.T) {
	c := validAWSConfig()
	var calls int
	sdk := aws.Config{Region: c.Region, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: ID(), SecretAccessKey: ID()}, nil
	}), HTTPClient: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Scheme != "https" || !strings.HasSuffix(r.URL.Host, ".amazonaws.com") {
			t.Fatal("non-AWS endpoint")
		}
		if r.Header.Get("Authorization") == "" {
			t.Fatal("unsigned AWS operation")
		}
		if r.Header.Get("x-amz-expected-bucket-owner") != c.Account {
			t.Fatal("bucket owner not pinned")
		}
		body := ""
		if r.Method == "PUT" && r.Header.Get("X-Amz-Server-Side-Encryption") != "AES256" {
			t.Fatal("object write omitted encryption")
		}
		if r.Method == "GET" {
			if r.URL.Query().Get("versionId") != "test-version" {
				t.Fatal("unpinned version read")
			}
			body = "synthetic"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Amz-Version-Id": []string{"test-version"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	o, e := newAWSObjects(c, sdk)
	if e != nil {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal("constructor contacted AWS")
	}
	if e = o.Init(context.Background(), "never-create"); e == nil {
		t.Fatal("cloud provisioning allowed through local init")
	}
	if calls != 0 {
		t.Fatal("cloud init made a request")
	}
	ref, e := o.Put(context.Background(), "jobs/test/input", []byte("synthetic"))
	if e != nil {
		t.Fatal("signed adapter write failed")
	}
	if _, e = o.Read(context.Background(), ref); e != nil {
		t.Fatal("signed pinned version read failed")
	}
	if calls != 2 {
		t.Fatal("unexpected request count")
	}
}
func TestAWSCheckReturnsSafeError(t *testing.T) {
	c := validAWSConfig()
	o, e := newAWSObjects(c, aws.Config{Region: c.Region, Credentials: aws.AnonymousCredentials{}, HTTPClient: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("sensitive transport details must not escape")
	})})
	if e != nil {
		t.Fatal(e)
	}
	e = o.Check(context.Background())
	if e == nil || strings.Contains(e.Error(), "sensitive") {
		t.Fatal("unsafe cloud diagnostics")
	}
}

func TestAWSPreflightChecksExistingResourcesReadOnly(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong-region", "unversioned", "public", "unencrypted-bucket", "expiring-versions", "unencrypted-queue", "wrong-queue-account", "missing-dlq", "unencrypted-dlq"} {
		t.Run(scenario, func(t *testing.T) {
			c := validAWSConfig()
			calls := 0
			sdk := aws.Config{Region: c.Region, Credentials: aws.AnonymousCredentials{}, HTTPClient: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				status := 200
				header := http.Header{}
				body := ""
				if strings.HasPrefix(r.URL.Host, "sqs.") {
					if r.Method != "POST" || !strings.HasSuffix(r.Header.Get("X-Amz-Target"), "GetQueueAttributes") {
						t.Fatal("preflight performed a queue mutation")
					}
					request, _ := io.ReadAll(r.Body)
					dead := strings.Contains(string(request), "processor-dead")
					name := "processor-work"
					if dead {
						name = "processor-dead"
					}
					arn := "arn:aws:sqs:eu-west-1:123456789012:" + name
					encryption := "true"
					if scenario == "unencrypted-queue" && !dead || scenario == "unencrypted-dlq" && dead {
						encryption = "false"
					}
					if scenario == "wrong-queue-account" && !dead {
						arn = "arn:aws:sqs:eu-west-1:999999999999:processor-work"
					}
					redrive := `{"deadLetterTargetArn":"arn:aws:sqs:eu-west-1:123456789012:processor-dead","maxReceiveCount":"20"}`
					if scenario == "missing-dlq" {
						redrive = ""
					}
					b, _ := json.Marshal(map[string]any{"Attributes": map[string]string{"QueueArn": arn, "SqsManagedSseEnabled": encryption, "VisibilityTimeout": "90", "RedrivePolicy": redrive}})
					body = string(b)
					header.Set("Content-Type", "application/x-amz-json-1.0")
				} else {
					if r.Method != "GET" && r.Method != "HEAD" {
						t.Fatal("preflight performed a bucket mutation")
					}
					if r.Header.Get("x-amz-expected-bucket-owner") != c.Account {
						t.Fatal("read omitted expected owner")
					}
					switch {
					case r.Method == "HEAD":
						header.Set("X-Amz-Bucket-Region", c.Region)
						if scenario == "wrong-region" {
							header.Set("X-Amz-Bucket-Region", "us-east-1")
						}
					case r.URL.Query().Has("versioning"):
						state := "Enabled"
						if scenario == "unversioned" {
							state = "Suspended"
						}
						body = "<VersioningConfiguration><Status>" + state + "</Status></VersioningConfiguration>"
					case r.URL.Query().Has("publicAccessBlock"):
						value := "true"
						if scenario == "public" {
							value = "false"
						}
						body = "<PublicAccessBlockConfiguration><BlockPublicAcls>" + value + "</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"
					case r.URL.Query().Has("encryption"):
						algorithm := "AES256"
						if scenario == "unencrypted-bucket" {
							algorithm = ""
						}
						body = "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>" + algorithm + "</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
					case r.URL.Query().Has("ownershipControls"):
						body = "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"
					case r.URL.Query().Has("lifecycle"):
						status = 404
						body = "<Error><Code>NoSuchLifecycleConfiguration</Code></Error>"
						if scenario == "expiring-versions" {
							status = 200
							body = "<LifecycleConfiguration><Rule><ID>unsafe</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"
						}
					default:
						t.Fatal("unexpected bucket preflight operation")
					}
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})}
			o, e := newAWSObjects(c, sdk)
			if e != nil {
				t.Fatal(e)
			}
			e = o.Check(context.Background())
			if scenario == "valid" && e != nil {
				t.Fatal(e)
			}
			if scenario != "valid" && e == nil {
				t.Fatal("unsafe resources accepted")
			}
			if calls == 0 {
				t.Fatal("no preflight contract checked")
			}
		})
	}
}

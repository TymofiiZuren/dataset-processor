package durable

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Config struct {
	Profile, Region, Account                           string
	Database, Endpoint, Bucket, Queue, Owner, Password string
	OwnerFile, PasswordFile                            string
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
func LocalConfig(init bool) (Config, error) {
	c := Config{Profile: "local", Database: env("PROCESSOR_DATABASE_URL", "postgres://postgres@127.0.0.1:15439/processor?sslmode=disable"), Endpoint: env("PROCESSOR_LOCAL_ENDPOINT", "http://127.0.0.1:15019"), Bucket: env("PROCESSOR_BUCKET", "processor-local"), Queue: env("PROCESSOR_QUEUE", "processor-local"), OwnerFile: env("PROCESSOR_OWNER_FILE", ".scratch/durable-owner"), PasswordFile: env("PROCESSOR_PASSWORD_FILE", ".scratch/durable-password")}
	var e error
	c.Owner, e = privateValue(c.OwnerFile, init)
	if e != nil {
		return c, e
	}
	c.Password, e = privateValue(c.PasswordFile, init)
	if e != nil {
		return c, e
	}
	return c, nil
}

// ProfileConfig keeps cloud targets explicit; no local defaults cross this boundary.
func ProfileConfig(profile string, init bool) (Config, error) {
	if profile == "cluster" {
		return clusterConfig()
	}
	if profile == "local" {
		return LocalConfig(init)
	}
	if profile != "aws" {
		return Config{}, errors.New("profile must be local, cluster, or aws")
	}
	c := Config{Profile: "aws", Region: os.Getenv("PROCESSOR_AWS_REGION"), Account: os.Getenv("PROCESSOR_AWS_ACCOUNT_ID"), Database: os.Getenv("PROCESSOR_DATABASE_URL"), Bucket: os.Getenv("PROCESSOR_BUCKET"), Queue: os.Getenv("PROCESSOR_QUEUE_URL"), Endpoint: os.Getenv("PROCESSOR_LOCAL_ENDPOINT"), OwnerFile: env("PROCESSOR_OWNER_FILE", ".scratch/aws-owner"), PasswordFile: env("PROCESSOR_PASSWORD_FILE", ".scratch/aws-password")}
	if e := c.ValidateAWS(); e != nil {
		return c, e
	}
	var e error
	c.Owner, e = privateValue(c.OwnerFile, init)
	if e != nil {
		return c, e
	}
	c.Password, e = privateValue(c.PasswordFile, init)
	return c, e
}
func (c Config) ValidateAWS() error {
	if c.Profile != "aws" || !regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]+$`).MatchString(c.Region) || strings.HasPrefix(c.Region, "cn-") || strings.HasPrefix(c.Region, "us-gov-") || strings.Contains(c.Region, "-iso") {
		return errors.New("AWS profile requires an explicit commercial PROCESSOR_AWS_REGION")
	}
	if !regexp.MustCompile(`^[0-9]{12}$`).MatchString(c.Account) {
		return errors.New("AWS profile requires PROCESSOR_AWS_ACCOUNT_ID")
	}
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`).MatchString(c.Bucket) || strings.Contains(c.Bucket, "..") || net.ParseIP(c.Bucket) != nil {
		return errors.New("AWS profile requires a standard S3 bucket name")
	}
	if c.Endpoint != "" {
		return errors.New("AWS profile refuses PROCESSOR_LOCAL_ENDPOINT")
	}
	for _, v := range os.Environ() {
		name, _, _ := strings.Cut(v, "=")
		if strings.HasPrefix(name, "AWS_ENDPOINT_URL") {
			return errors.New("AWS profile refuses AWS_ENDPOINT_URL overrides; use local profile for emulation")
		}
	}
	u, e := url.Parse(c.Queue)
	if e != nil || u.Scheme != "https" || u.Host != "sqs."+c.Region+".amazonaws.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return errors.New("PROCESSOR_QUEUE_URL must be an HTTPS SQS URL in the configured region/account")
	}
	pieces := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(pieces) != 2 || pieces[0] != c.Account || !regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`).MatchString(pieces[1]) {
		return errors.New("AWS profile requires a standard SQS queue in the configured account")
	}
	if c.Database == "" {
		return errors.New("AWS profile requires explicit PROCESSOR_DATABASE_URL; use a dedicated database")
	}
	return nil
}
func (c Config) Backend() string {
	if c.Profile == "aws" {
		return "aws:" + c.Account + ":" + c.Region + ":" + c.Bucket + ":" + c.Queue
	}
	return "local:" + c.Endpoint + ":" + c.Bucket + ":" + c.Queue
}
func privateValue(path string, init bool) (string, error) {
	if init {
		if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			return "", errors.New("cannot create private profile configuration")
		}
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e == nil {
			_, w := f.WriteString(ID())
			cl := f.Close()
			if w != nil || cl != nil {
				return "", errors.New("cannot save private profile configuration")
			}
		} else if !os.IsExist(e) {
			return "", errors.New("cannot initialize private profile configuration")
		}
	}
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", errors.New("private profile configuration missing or unsafe permissions; run config init for the selected profile")
	}
	b, e := os.ReadFile(path)
	if e != nil || len(strings.TrimSpace(string(b))) < 32 {
		return "", errors.New("invalid private profile configuration")
	}
	return strings.TrimSpace(string(b)), nil
}
func Connect(ctx context.Context, c Config) (*Runtime, error) {
	s, e := OpenProfile(ctx, c.Database, c.Profile)
	if e != nil {
		return nil, e
	}
	var o *Objects
	if c.Profile == "aws" {
		o, e = NewAWSObjects(ctx, c)
	} else if c.Profile == "cluster" {
		o, e = NewClusterObjects(ctx, c.Endpoint, c.Bucket, c.Queue)
	} else {
		o, e = NewObjects(ctx, c.Endpoint, c.Bucket, c.Queue)
	}
	if e != nil {
		s.Close()
		return nil, e
	}
	return &Runtime{Store: s, Objects: o, Owner: c.Owner}, nil
}

const ClusterEndpoint = "http://moto.dataset-processor.svc.cluster.local:5000"

func clusterConfig() (Config, error) {
	c := Config{Profile: "cluster", Database: os.Getenv("PROCESSOR_DATABASE_URL"), Endpoint: ClusterEndpoint, Bucket: "processor-cluster", Queue: "processor-cluster", OwnerFile: os.Getenv("PROCESSOR_OWNER_FILE"), PasswordFile: os.Getenv("PROCESSOR_PASSWORD_FILE")}
	if v := os.Getenv("PROCESSOR_LOCAL_ENDPOINT"); v != "" && v != ClusterEndpoint {
		return c, errors.New("cluster emulator endpoint is fixed")
	}
	if _, e := databaseConfig(c.Database, "cluster"); e != nil {
		return c, e
	}
	var e error
	c.Owner, e = privateValue(c.OwnerFile, false)
	if e != nil {
		return c, e
	}
	c.Password, e = privateValue(c.PasswordFile, false)
	return c, e
}

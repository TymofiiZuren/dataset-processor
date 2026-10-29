package durable

import (
	"context"
	"dataset-processor/internal/cluster"
	"os"
	"path/filepath"
	"testing"
)

func TestClusterBoundaries(t *testing.T) {
	if _, e := databaseConfig("postgres://processor@unrelated:5432/processor?sslmode=disable", "cluster"); e == nil {
		t.Fatal("accepted unrelated database")
	}
	if _, e := databaseConfig("postgres://processor@"+cluster.DatabaseHost+"/processor?sslmode=disable", "cluster"); e == nil {
		t.Fatal("accepted unverified TLS")
	}
	if _, e := NewClusterObjects(context.Background(), "http://unrelated:5000", "bucket", "queue"); e == nil {
		t.Fatal("accepted unrelated emulator")
	}
	if _, e := NewObjects(context.Background(), ClusterEndpoint, "bucket", "queue"); e == nil {
		t.Fatal("local guard weakened")
	}
	if e := os.MkdirAll("../../.scratch", 0700); e != nil {
		t.Fatal(e)
	}
	d, e := os.MkdirTemp("../../.scratch", "cluster-config-")
	if e != nil {
		t.Fatal(e)
	}
	d = filepath.Join(d, "private")
	if e = cluster.Init(d); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PROCESSOR_OWNER_FILE", filepath.Join(d, "owner"))
	t.Setenv("PROCESSOR_PASSWORD_FILE", filepath.Join(d, "password"))
	t.Setenv("PGSSLROOTCERT", filepath.Join(d, "ca.crt"))
	t.Setenv("PGPASSFILE", filepath.Join(d, "pgpass"))
	t.Setenv("PROCESSOR_DATABASE_URL", "postgres://processor@"+cluster.DatabaseHost+":5432/processor?sslmode=verify-full")
	c, e := ProfileConfig("cluster", false)
	if e != nil {
		t.Fatal(e)
	}
	if c.Endpoint != ClusterEndpoint {
		t.Fatal("cluster endpoint mismatch")
	}
	if _, e = databaseConfig(c.Database, "cluster"); e != nil {
		t.Fatal(e)
	}
}

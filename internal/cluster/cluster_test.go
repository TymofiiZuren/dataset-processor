package cluster

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func testDir(t *testing.T) string {
	t.Helper()
	if e := os.MkdirAll("../../.scratch", 0700); e != nil {
		t.Fatal(e)
	}
	d, e := os.MkdirTemp("../../.scratch", "cluster-test-")
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func TestPrivateBundleIsStableAndCertificateMatchesService(t *testing.T) {
	d := filepath.Join(testDir(t), "private")
	if e := Init(d); e != nil {
		t.Fatal(e)
	}
	first, e := Digest(d)
	if e != nil {
		t.Fatal(e)
	}
	if e = Init(d); e != nil {
		t.Fatal(e)
	}
	second, _ := Digest(d)
	if first != second {
		t.Fatal("initialization rotated existing identity")
	}
	for _, name := range Files {
		info, e := os.Lstat(filepath.Join(d, name))
		if e != nil || info.Mode().Perm() != 0600 {
			t.Fatal("private bundle permission failure")
		}
	}
	data, e := os.ReadFile(filepath.Join(d, "server.crt"))
	if e != nil {
		t.Fatal(e)
	}
	block, _ := pem.Decode(data)
	cert, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		t.Fatal(e)
	}
	rootData, _ := os.ReadFile(filepath.Join(d, "ca.crt"))
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(rootData)
	if _, e = cert.Verify(x509.VerifyOptions{DNSName: DatabaseHost, Roots: roots}); e != nil {
		t.Fatal("cluster database certificate invalid")
	}
	if e = cert.VerifyHostname("unrelated.example"); e == nil {
		t.Fatal("database identity not restricted")
	}
	staged := filepath.Join(testDir(t), "staged")
	if e = Stage(d, staged); e != nil {
		t.Fatal(e)
	}
	if e = Stage(d, staged); e != nil {
		t.Fatal("stage rerun not idempotent")
	}
	if e = os.WriteFile(filepath.Join(d, "password"), []byte("changed"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = Stage(d, staged); e == nil {
		t.Fatal("stage silently replaced live identity")
	}
}
func TestBundleRejectsIncompleteAndUnsafeFiles(t *testing.T) {
	d := filepath.Join(testDir(t), "incomplete")
	if e := os.Mkdir(d, 0700); e != nil {
		t.Fatal(e)
	}
	if e := Init(d); e == nil {
		t.Fatal("accepted incomplete bundle")
	}
	d = filepath.Join(testDir(t), "private")
	if e := Init(d); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(filepath.Join(d, "password"), 0644); e != nil {
		t.Fatal(e)
	}
	if _, e := Digest(d); e == nil {
		t.Fatal("accepted public credential file")
	}
}

func TestStageRejectsSymlinkDestinationAndMissingSource(t *testing.T) {
	source := filepath.Join(testDir(t), "source")
	if e := Init(source); e != nil {
		t.Fatal(e)
	}
	root := testDir(t)
	target := filepath.Join(root, "target")
	if e := os.Mkdir(target, 0700); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(root, "link")
	if e := os.Symlink(target, link); e != nil {
		t.Fatal(e)
	}
	if e := Stage(source, link); e == nil {
		t.Fatal("accepted symlink destination")
	}
	if e := Stage(filepath.Join(root, "missing"), filepath.Join(root, "staged")); e == nil {
		t.Fatal("accepted missing projection")
	}
}

func TestResourcesKeepOwnerAPIPrivateAndRolesRestricted(t *testing.T) {
	list, e := Resources("dataset-processor:test", 8089, "abc12345", "all")
	if e != nil {
		t.Fatal(e)
	}
	roles := map[string]bool{}
	for _, r := range list.Items {
		kind := r["kind"].(string)
		name := r["metadata"].(map[string]any)["name"].(string)
		if kind == "Secret" {
			t.Fatal("manifest includes secrets")
		}
		if kind == "Service" && name == "processor-api" {
			t.Fatal("API exposed through service")
		}
		if kind == "Deployment" && name != "moto" {
			roles[name] = true
			p := r["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
			if p["automountServiceAccountToken"] != false || p["securityContext"].(map[string]any)["runAsNonRoot"] != true {
				t.Fatal("unsafe role permissions")
			}
		}
	}
	if len(roles) != 3 {
		t.Fatal("three role deployments required")
	}
	if _, e = Resources("image;command", 8089, "abc", "all"); e == nil {
		t.Fatal("invalid image accepted")
	}
}

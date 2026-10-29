// Package cluster prepares private, local Kubernetes bootstrap material.
package cluster

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

const DatabaseHost = "postgres.dataset-processor.svc.cluster.local"

var Files = []string{"owner", "password", "db-password", "pgpass", "ca.crt", "ca.key", "server.crt", "server.key"}
var stageFiles = []string{"owner", "password", "pgpass", "ca.crt"}

// Init never rotates an existing identity. Interrupted initialization is rejected.
func Init(dir string) error {
	if _, err := os.Lstat(dir); err == nil {
		if err = privateDirectory(dir); err != nil {
			return err
		}
		if _, err = readPrivate(filepath.Join(dir, "complete")); err != nil {
			return errors.New("incomplete cluster bundle; choose a new directory")
		}
		_, err = Digest(dir)
		return err
	} else if !os.IsNotExist(err) {
		return errors.New("cannot inspect cluster bundle")
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	values := map[string][]byte{}
	for _, name := range []string{"owner", "password", "db-password"} {
		b := make([]byte, 32)
		if _, e := rand.Read(b); e != nil {
			return e
		}
		values[name] = []byte(hex.EncodeToString(b))
	}
	values["pgpass"] = []byte(DatabaseHost + ":5432:processor:processor:" + string(values["db-password"]) + "\n")
	caKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return e
	}
	serverKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return e
	}
	caSerial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 159))
	if e != nil {
		return e
	}
	serverSerial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 159))
	if e != nil {
		return e
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: caSerial, Subject: pkix.Name{CommonName: "Dataset processor local cluster CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(1, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	cert := &x509.Certificate{SerialNumber: serverSerial, Subject: pkix.Name{CommonName: DatabaseHost}, DNSNames: []string{DatabaseHost}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(0, 0, 90), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	for _, item := range []struct {
		name             string
		template, parent *x509.Certificate
		public           *ecdsa.PublicKey
		signing          *ecdsa.PrivateKey
	}{{"ca.crt", ca, ca, &caKey.PublicKey, caKey}, {"server.crt", cert, ca, &serverKey.PublicKey, caKey}} {
		der, err := x509.CreateCertificate(rand.Reader, item.template, item.parent, item.public, item.signing)
		if err != nil {
			return err
		}
		values[item.name] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	for name, key := range map[string]*ecdsa.PrivateKey{"ca.key": caKey, "server.key": serverKey} {
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return err
		}
		values[name] = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	}
	for _, name := range Files {
		if e = writeNew(filepath.Join(dir, name), values[name]); e != nil {
			return e
		}
	}
	return writeNew(filepath.Join(dir, "complete"), []byte("1\n"))
}
func readPrivate(path string) ([]byte, error) {
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("cluster bundle missing or unsafe permissions")
	}
	b, e := os.ReadFile(path)
	if e != nil || len(b) == 0 {
		return nil, errors.New("cluster bundle unreadable or empty")
	}
	return b, nil
}
func writeNew(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return errors.New("cannot create private cluster file")
	}
	_, w := f.Write(b)
	c := f.Close()
	if w != nil || c != nil {
		return errors.New("cannot save private cluster file")
	}
	return nil
}

// Digest checks regular private files without returning their contents.
func Digest(dir string) (string, error) {
	if e := privateDirectory(dir); e != nil {
		return "", e
	}
	h := sha256.New()
	for _, name := range Files {
		b, e := readPrivate(filepath.Join(dir, name))
		if e != nil {
			return "", e
		}
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write(b)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Stage copies projected Secret values into regular private files. Kubernetes
// projections may use symlinks; the destination never accepts symlinks or rotation.
func Stage(from, to string) error {
	if e := os.MkdirAll(to, 0700); e != nil {
		return errors.New("cannot create private staging directory")
	}
	if e := privateDirectory(to); e != nil {
		return e
	}
	for _, name := range stageFiles {
		b, e := os.ReadFile(filepath.Join(from, name))
		if e != nil || len(b) == 0 {
			return errors.New("projected cluster configuration missing")
		}
		path := filepath.Join(to, name)
		if _, e = os.Lstat(path); e == nil {
			existing, err := readPrivate(path)
			if err != nil || !bytes.Equal(existing, b) {
				return errors.New("staged identity differs; refusing replacement")
			}
		} else if !os.IsNotExist(e) {
			return errors.New("cannot inspect staged identity")
		} else if e = writeNew(path, b); e != nil {
			return e
		}
	}
	return nil
}

func privateDirectory(path string) error {
	info, e := os.Lstat(path)
	if e != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("cluster configuration directory must be private and cannot be a symlink")
	}
	return nil
}

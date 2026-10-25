package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"dataset-processor/internal/processor"
	"dataset-processor/internal/web"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "processor:", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: processor serve | process | verify | doctor (use -help for command options)")
	}
	switch args[0] {
	case "cluster":
		return clusterCommand(args[1:])
	case "serve":
		return serve(args[1:])
	case "process", "verify":
		return process(args[0], args[1:])
	case "api", "coordinator", "worker", "migrate":
		return durableCommand(args[0], args[1:])
	case "demo":
		if len(args) < 2 || args[1] != "recovery" {
			return errors.New("usage: processor demo recovery --profile local")
		}
		return durableCommand("demo-recovery", args[2:])
	case "config":
		if len(args) < 2 || args[1] != "init" {
			return errors.New("usage: processor config init --profile local|aws")
		}
		return durableCommand("config-init", args[2:])
	case "local":
		if len(args) < 2 || args[1] != "init" {
			return errors.New("usage: processor local init --profile local")
		}
		return durableCommand("local-init", args[2:])
	case "job":
		if len(args) < 2 || (args[1] != "inspect" && args[1] != "submit" && args[1] != "fetch") {
			return errors.New("usage: processor job inspect|submit|fetch (use -help for options)")
		}
		return durableCommand("job-"+args[1], args[2:])
	case "doctor":
		if len(args) > 1 {
			return durableCommand("durable-doctor", args[1:])
		}
		fmt.Println("Local reference processor ·", processor.Version, "\nNo AWS credentials or services required.\nFor durable dependency checks use doctor --profile local.")
		return nil
	default:
		return errors.New("unknown command; use serve, process, verify, doctor, local init, migrate, api, coordinator, worker, or job inspect")
	}
}
func process(command string, args []string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	input := flags.String("input", "", "input transaction CSV")
	output := flags.String("output", "", "new output directory (must not exist)")
	expected := flags.String("expected", "", "directory containing independently expected clean.csv and rejected.csv (required for verify)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *input == "" || *output == "" {
		return errors.New("provide --input and --output; outputs are never overwritten")
	}
	if command == "verify" && *expected == "" {
		return errors.New("verify requires --expected with independently authored clean.csv and rejected.csv")
	}
	if _, err := os.Lstat(*output); !errors.Is(err, os.ErrNotExist) {
		return errors.New("output path already exists or cannot be checked; choose a new directory")
	}
	file, err := os.Open(*input)
	if err != nil {
		return errors.New("cannot open input file")
	}
	defer file.Close()
	if err = os.MkdirAll(".scratch", 0700); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(".scratch", "processing-")
	if err != nil {
		return err
	}
	clean, err := os.OpenFile(filepath.Join(stage, "clean.csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	rejected, err := os.OpenFile(filepath.Join(stage, "rejected.csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		clean.Close()
		return err
	}
	manifest, processErr := processor.Process(file, clean, rejected, processor.DefaultLimits())
	closeErr := errors.Join(clean.Close(), rejected.Close())
	if processErr != nil {
		return fmt.Errorf("%w; no result published (private staging retained under .scratch)", processErr)
	}
	if closeErr != nil {
		return errors.New("output write failed; no result published")
	}
	verified := false
	if command == "verify" {
		for name, want := range map[string]string{"clean.csv": manifest.CleanSHA256, "rejected.csv": manifest.RejectedSHA256} {
			f, e := os.Open(filepath.Join(*expected, name))
			if e != nil {
				return errors.New("cannot open independently expected output")
			}
			h := sha256.New()
			_, e = io.Copy(h, f)
			f.Close()
			if e != nil {
				return errors.New("cannot read expected output")
			}
			if hex.EncodeToString(h.Sum(nil)) != want {
				return fmt.Errorf("reference_mismatch: %s differs from expected output; no result published", name)
			}
		}
		verified = true
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err = os.WriteFile(filepath.Join(stage, "manifest.json"), data, 0600); err != nil {
		return err
	}
	// Reserve a brand-new output directory. Publish the completion manifest last.
	// A crash during moves can leave an incomplete directory, never a false success.
	if err = os.Mkdir(*output, 0700); err != nil {
		return errors.New("cannot create new output directory; choose an existing parent and unused name")
	}
	for _, name := range []string{"clean.csv", "rejected.csv", "manifest.json"} {
		if err = os.Rename(filepath.Join(stage, name), filepath.Join(*output, name)); err != nil {
			return errors.New("publication interrupted; output is incomplete unless manifest.json exists")
		}
	}
	fmt.Printf("Records: %d · accepted: %d · rejected: %d\n", manifest.Records, manifest.Accepted, manifest.Rejected)
	if verified {
		fmt.Println("Verification: PASS (byte-for-byte hashes match independently expected outputs)")
	}
	fmt.Println("Results:", *output, "· single-process reference, no cloud services")
	return nil
}
func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := flags.String("addr", "127.0.0.1:0", "IPv4 loopback address (port 0 selects a free port)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(*addr)
	if err != nil || host != "127.0.0.1" {
		return errors.New("serve only supports 127.0.0.1; use a private connection, never a public bind")
	}
	listener, err := net.Listen("tcp4", *addr)
	if err != nil {
		return errors.New("cannot bind requested address; choose a different port")
	}
	defer listener.Close()
	password := os.Getenv("PROCESSOR_OWNER_PASSWORD")
	if password == "" {
		var secret [32]byte
		if _, err = rand.Read(secret[:]); err != nil {
			return err
		}
		password = hex.EncodeToString(secret[:])
		if err = os.MkdirAll(".scratch", 0700); err != nil {
			return err
		}
		f, e := os.CreateTemp(".scratch", "owner-password-")
		if e != nil {
			return e
		}
		_, writeErr := io.WriteString(f, password)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			return errors.New("cannot create private owner password file")
		}
		path, e := filepath.Abs(f.Name())
		if e != nil {
			return e
		}
		fmt.Println("Owner password file:", path, "(private; open locally to sign in)")
	}
	handler, err := web.New(password, listener.Addr().String())
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10}
	fmt.Println("Workspace: http://" + listener.Addr().String())
	fmt.Println("Local reference mode · 8 MiB web upload limit · no AWS resources")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-done:
		}
	}()
	err = server.Serve(listener)
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

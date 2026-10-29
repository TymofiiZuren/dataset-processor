package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"dataset-processor/internal/durable"
	"dataset-processor/internal/web"
)

func durableCommand(command string, args []string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	addr := flags.String("addr", "127.0.0.1:0", "loopback workspace address; port 0 chooses a free port")
	profile := flags.String("profile", "local", "backend profile: local, cluster, or aws (AWS targets must be explicitly configured)")
	checkpoint := flags.String("checkpoint", "", "local recovery experiment: pause after claiming chunk zero")
	lease := flags.Duration("lease", time.Minute, "local claim lease duration")
	summary := flags.Bool("summary", false, "safe recovery summary without fencing values")
	input := flags.String("input", "", "local demonstration input file")
	key := flags.String("key", "", "submission idempotency key")
	name := flags.String("name", "", "download name")
	chunkBytes := flags.Int("chunk-bytes", 0, "local demonstration chunk target; zero uses the normal target")
	job := flags.String("job", "", "job identifier for read-only inspect")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if *profile != "local" && *profile != "aws" && *profile != "cluster" {
		return errors.New("profile must be local, cluster, or aws")
	}
	if *profile == "aws" && (command == "local-init" || command == "demo-recovery" || *checkpoint != "" || *lease != time.Minute || command == "worker" && *job != "" || *chunkBytes != 0 || command == "job-submit" || command == "job-fetch") {
		return errors.New("local init/recovery/debug flags refuse AWS profile; provision resources separately")
	}

	if *profile == "cluster" && (command == "config-init" || command == "demo-recovery") {
		return errors.New("cluster uses cluster init and the Kubernetes recovery script")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if command == "durable-doctor" && *profile == "local" {
		probe := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}")
		if e := probe.Run(); e != nil {
			return errors.New("DEPENDENCY_UNAVAILABLE: Docker daemon is not reachable; start your container runtime and rerun doctor --profile local")
		}
	}
	init := command == "local-init" || command == "config-init"
	c, e := durable.ProfileConfig(*profile, init)
	if e != nil {
		return e
	}
	if command == "config-init" {
		path, _ := filepath.Abs(c.PasswordFile)
		fmt.Println("Private profile identity initialized; no database or AWS request made.")
		fmt.Println("Owner password file:", path, "(private; open locally)")
		return nil
	}
	if command == "migrate" {
		store, e := durable.OpenProfile(ctx, c.Database, c.Profile)
		if e != nil {
			return e
		}
		defer store.Close()
		if e = store.Migrate(ctx); e != nil {
			return errors.New("migration failed; check schema compatibility; no destructive reset performed")
		}
		if e = store.BindBackend(ctx, c.Owner, c.Backend(), c.Profile == "local"); e != nil {
			return e
		}
		fmt.Println("Schema version 2 ready; owner backend binding verified.")
		return nil
	}
	r, e := durable.Connect(ctx, c)
	if e != nil {
		return e
	}
	defer r.Store.Close()
	if command == "local-init" {
		if e = r.Store.Migrate(ctx); e != nil {
			return errors.New("migration failed; inspect compatibility before retrying")
		}
		if e = r.Store.BindBackend(ctx, c.Owner, c.Backend(), true); e != nil {
			return e
		}
	}
	if command == "local-init" {
		if e = r.Objects.Init(ctx, c.Queue); e != nil {
			return e
		}
		path, _ := filepath.Abs(c.PasswordFile)
		fmt.Println("Local state initialized; reruns preserve existing owner and jobs.")
		fmt.Println("Owner password file:", path, "(private; open locally)")
		return nil
	}
	checkCtx, cancelCheck := context.WithTimeout(ctx, 45*time.Second)
	e = r.Store.Check(checkCtx)
	if e == nil {
		e = r.CheckBackend(checkCtx)
	}
	if e == nil {
		e = r.Objects.Check(checkCtx)
	}
	cancelCheck()
	if e != nil {
		return e
	}

	if *chunkBytes != 0 {
		if command != "coordinator" || *chunkBytes < 80 || *chunkBytes > 16<<20 {
			return errors.New("chunk target requires local coordinator and 80..16777216 bytes")
		}
		r.ChunkBytes = *chunkBytes
	}
	switch command {
	case "job-submit":
		if *input == "" || *key == "" {
			return errors.New("submit requires --input and --key")
		}
		f, e := os.Open(*input)
		if e != nil {
			return errors.New("input unavailable")
		}
		defer f.Close()
		b, e := io.ReadAll(io.LimitReader(f, durable.MaxInput+1))
		if e != nil || len(b) > durable.MaxInput {
			return errors.New("input unreadable or exceeds limit")
		}
		j, e := r.Submit(ctx, *key, b)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(recoverySummary(j, nil))
	case "job-fetch":
		j, e := r.Store.Get(ctx, r.Owner, *job)
		if e != nil {
			return errors.New("job unavailable")
		}
		return r.Download(ctx, j, *name, os.Stdout)

	case "durable-doctor":
		if *profile == "aws" {
			fmt.Println("AWS preflight passed: configured bucket/queues and PostgreSQL backend verified. Processing/recovery still requires a separate acceptance run.")
		} else {
			fmt.Println("Local durable profile ready: PostgreSQL schema, versioned S3 emulation, SQS queue. No AWS fallback.")
		}
		return nil
	case "job-inspect":
		if *job == "" {
			return errors.New("provide --job")
		}
		if *summary {
			j, e := r.Store.Get(ctx, r.Owner, *job)
			if e != nil {
				return errors.New("job unavailable")
			}
			chunks, e := r.Store.Chunks(ctx, j)
			if e != nil {
				return e
			}
			return json.NewEncoder(os.Stdout).Encode(recoverySummary(j, chunks))
		}
		v, e := r.Store.Diagnostic(ctx, r.Owner, *job)
		if e != nil {
			return errors.New("job unavailable for this owner")
		}
		return json.NewEncoder(os.Stdout).Encode(v)
	case "demo-recovery":
		return recoveryDemo(ctx, r, c)
	case "coordinator", "worker":
		if *lease < time.Second || *lease > time.Minute {
			return errors.New("local lease must be between one second and one minute")
		}
		r.Lease = *lease
		if command == "worker" && *job != "" {
			return scenarioWorker(ctx, r, *job, *checkpoint)
		}
		fmt.Printf("Role: %s · profile: %s\n", command, *profile)
		return r.Run(ctx, command)
	case "api":
		host, _, e := net.SplitHostPort(*addr)
		if e != nil || host != "127.0.0.1" {
			return errors.New("api only supports IPv4 loopback; public binds refused")
		}
		listener, e := net.Listen("tcp4", *addr)
		if e != nil {
			return errors.New("address unavailable; choose another port")
		}
		defer listener.Close()
		handler, e := web.NewDurable(c.Password, listener.Addr().String(), r)
		if e != nil {
			return e
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10}
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ctx.Done():
				c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(c)
			case <-done:
			}
		}()
		path, _ := filepath.Abs(c.PasswordFile)
		fmt.Println("Owner password file:", path, "(private; open locally)")
		fmt.Println("Workspace: http://" + listener.Addr().String())
		fmt.Printf("Durable %s mode · loopback owner access · start coordinator and worker separately\n", *profile)
		e = server.Serve(listener)
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	}
	return errors.New("unknown durable command")
}

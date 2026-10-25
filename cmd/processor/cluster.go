package main

import (
	"dataset-processor/internal/cluster"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
)

func clusterCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: processor cluster init|stage|manifest|digest (use -help for options)")
	}
	flags := flag.NewFlagSet("cluster "+args[0], flag.ContinueOnError)
	switch args[0] {
	case "manifest":
		image := flags.String("image", "", "locally loaded application image")
		port := flags.Int("port", 8089, "loopback API and matching port-forward port")
		id := flags.String("run-id", "", "bootstrap job ID")
		phase := flags.String("phase", "all", "dependencies, bootstrap, roles, or all")
		if e := flags.Parse(args[1:]); e != nil {
			return e
		}
		list, e := cluster.Resources(*image, *port, *id, *phase)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(list)
	case "digest":
		dir := flags.String("directory", "", "private bundle directory")
		if e := flags.Parse(args[1:]); e != nil {
			return e
		}
		digest, e := cluster.Digest(*dir)
		if e != nil {
			return e
		}
		fmt.Println(digest)
		return nil

	case "init":
		output := flags.String("output", "", "new private bootstrap directory; existing complete identity is preserved")
		if e := flags.Parse(args[1:]); e != nil {
			return e
		}
		if *output == "" || flags.NArg() != 0 {
			return errors.New("cluster init requires --output DIRECTORY")
		}
		if e := cluster.Init(*output); e != nil {
			return e
		}
		fmt.Println("Private cluster bootstrap ready. Credentials remain in the selected directory.")
		return nil
	case "stage":
		from := flags.String("from", "", "trusted projected Secret directory")
		to := flags.String("to", "", "private runtime configuration directory")
		if e := flags.Parse(args[1:]); e != nil {
			return e
		}
		if *from == "" || *to == "" || flags.NArg() != 0 {
			return errors.New("cluster stage requires --from DIRECTORY --to DIRECTORY")
		}
		return cluster.Stage(*from, *to)
	default:
		return errors.New("unknown cluster command; use init, stage, manifest, or digest")
	}
}

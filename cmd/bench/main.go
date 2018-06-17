package main

import (
	"fmt"
	"os"

	"github.com/hatlonely/kvclient/pkg/kvcfg"
	"github.com/spf13/pflag"
)

// AppVersion version info from scripts/version.sh
var AppVersion = "unknown"

func main() {
	version := pflag.BoolP("version", "v", false, "print current version")
	config := pflag.StringP("filename", "f", "configs/kvloader.json", "configuration filename")
	pflag.Parse()
	if *version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	loader, err := kvcfg.NewKVBenchmarkerWithFile(*config)
	if err != nil {
		panic(err)
	}

	if err := loader.Benchmark(); err != nil {
		panic(err)
	}
}

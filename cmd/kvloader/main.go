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
	pflag.String("producer.s3suffix", "", "suffix for s3path. usually datetime")
	pflag.Parse()
	if *version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	loader, err := kvcfg.NewKVLoaderWithFile(*config)
	if err != nil {
		panic(err)
	}

	if err := loader.Load(); err != nil {
		panic(err)
	}
}

package kvcfg

import (
	"os"

	"github.com/hatlonely/kvclient/pkg/kvbench"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// NewKVBenchmarkerWithFile create a new kv benchmarker use config file
func NewKVBenchmarkerWithFile(filename string) (*kvbench.KVBenchmarker, error) {
	config := viper.New()
	config.SetConfigType("json")
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	if err := config.ReadConfig(fp); err != nil {
		return nil, err
	}

	config.BindPFlags(pflag.CommandLine)
	return NewKVBenchmarker(config)
}

// NewKVBenchmarker create a new benchmarker
func NewKVBenchmarker(config *viper.Viper) (*kvbench.KVBenchmarker, error) {
	builder := kvbench.NewKVBenchmarkerBuilder()
	if err := config.Unmarshal(builder); err != nil {
		return nil, err
	}

	kvclient, err := NewKVClient(config.Sub("kvclient"))
	if err != nil {
		return nil, err
	}
	producer, err := NewKVProducer(config.Sub("producer"))
	if err != nil {
		return nil, err
	}

	return builder.
		WithKVClient(kvclient).
		WithProducer(producer).
		Build(), nil
}

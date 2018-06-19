package kvbench

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hatlonely/kvclient/pkg/kvclient"
	"github.com/hatlonely/kvclient/pkg/kvloader"
)

// NewKVBenchmarkerBuilder create a new kv benchmarker builder
func NewKVBenchmarkerBuilder() *KVBenchmarkerBuilder {
	return &KVBenchmarkerBuilder{
		TimeDistributionThreshold: []time.Duration{
			time.Duration(300) * time.Microsecond,
			time.Duration(500) * time.Microsecond,
			time.Duration(800) * time.Microsecond,
			time.Duration(1000) * time.Microsecond,
			time.Duration(2000) * time.Microsecond,
			time.Duration(5000) * time.Microsecond,
		},
	}
}

// KVBenchmarkerBuilder kv benchmarker builder
type KVBenchmarkerBuilder struct {
	TimeDistributionThreshold []time.Duration
	Schedule                  []*ScheduleItem
	kvclient                  kvclient.KVClient
	producer                  kvloader.KVProducer
}

// WithTimeDistributionThreshold option
func (b *KVBenchmarkerBuilder) WithTimeDistributionThreshold(timeDistributionThreshold []time.Duration) *KVBenchmarkerBuilder {
	b.TimeDistributionThreshold = timeDistributionThreshold
	return b
}

// WithKVClient option
func (b *KVBenchmarkerBuilder) WithKVClient(client kvclient.KVClient) *KVBenchmarkerBuilder {
	b.kvclient = client
	return b
}

// WithSchedule option
func (b *KVBenchmarkerBuilder) WithSchedule(schedule []*ScheduleItem) *KVBenchmarkerBuilder {
	b.Schedule = schedule
	return b
}

// WithProducer option
func (b *KVBenchmarkerBuilder) WithProducer(producer kvloader.KVProducer) *KVBenchmarkerBuilder {
	b.producer = producer
	return b
}

// Build option
func (b *KVBenchmarkerBuilder) Build() *KVBenchmarker {
	return &KVBenchmarker{
		timeDistributionThreshold: b.TimeDistributionThreshold,
		kvclient:                  b.kvclient,
		schedule:                  b.Schedule,
		producer:                  b.producer,
	}
}

// ScheduleItem run schedule
type ScheduleItem struct {
	ReaderNum    int
	WriterNum    int
	StartPercent int
	EndPercent   int
	Times        int
}

// KVBenchmarker benchmark kv storage
type KVBenchmarker struct {
	timeDistributionThreshold []time.Duration
	kvclient                  kvclient.KVClient
	schedule                  []*ScheduleItem
	producer                  kvloader.KVProducer
}

// Benchmark run benchmark
func (b *KVBenchmarker) Benchmark() error {
	mem := kvloader.NewMemKVConsumerBuilder().Build()

	loader := kvloader.NewBuilder().
		WithProducer(b.producer).
		WithConsumer(mem).
		Build()

	if err := loader.Load(); err != nil {
		return err
	}

	timeDisArrayStr := make([]string, len(b.timeDistributionThreshold))
	for i := range b.timeDistributionThreshold {
		timeDisArrayStr[i] = fmt.Sprintf("%v", b.timeDistributionThreshold[i])
	}
	fmt.Printf("\t\t%v\t%v\t%v\t% 8v\t% 8v\t% 8v\t%v\n", "succ", "fail", "totalTime", "qps", "res_time", strings.Join(timeDisArrayStr, "\t"), `succ%`)
	l := len(mem.Infos)
	for _, item := range b.schedule {
		if item.Times <= 0 {
			item.Times = 1
		}
		for i := 0; i < item.Times; i++ {
			b.BenchmarkMultiThread(item.ReaderNum, item.WriterNum, mem.Infos[item.StartPercent*l/100:item.EndPercent*l/100])
		}
	}
	return nil
}

// BenchmarkMultiThread benchmark with multi thread
func (b *KVBenchmarker) BenchmarkMultiThread(readerNum int, writerNum int, infos []*kvloader.KVInfo) {
	var wg sync.WaitGroup
	kpis := make(chan *KPI, readerNum+writerNum)
	l := len(infos)
	n := readerNum + writerNum
	i := 0
	for ; i < readerNum; i++ {
		go func(i int) {
			kpis <- b.BenchmarkGet(infos[i*l/n : (i+1)*l/n])
			wg.Done()
		}(i)
		wg.Add(1)
	}

	for ; i < n; i++ {
		go func(i int) {
			kpis <- b.BenchmarkSet(infos[i*l/n : (i+1)*l/n])
			wg.Done()
		}(i)
		wg.Add(1)
	}

	wg.Wait()
	close(kpis)

	kpiMap := map[string]*KPI{}
	for kpi := range kpis {
		if _, ok := kpiMap[kpi.option]; !ok {
			kpiMap[kpi.option] = &KPI{kpi.option, 0, 0, 0, 0, make([]int, len(b.timeDistributionThreshold))}
		}
		kpiMap[kpi.option].success += kpi.success
		kpiMap[kpi.option].fail += kpi.fail
		kpiMap[kpi.option].totalTime += kpi.totalTime
		kpiMap[kpi.option].count += kpi.count
		for i := range kpi.timeDistribution {
			kpiMap[kpi.option].timeDistribution[i] += kpi.timeDistribution[i]
		}
	}

	key := fmt.Sprintf("Get-%v-Set-%v", readerNum, writerNum)
	if readerNum == 0 {
		key = fmt.Sprintf("Set-%v", writerNum)
	} else if writerNum == 0 {
		key = fmt.Sprintf("Get-%v", readerNum)
	}

	for option, kpi := range kpiMap {
		kpi.option = fmt.Sprintf("%v-%v", key, option)
		fmt.Println(kpi.Show())
	}
}

// BenchmarkSet benchmark for set
func (b *KVBenchmarker) BenchmarkSet(infos []*kvloader.KVInfo) *KPI {
	totalTime := time.Duration(0)
	success := 0
	fail := 0
	timeDistribution := make([]int, len(b.timeDistributionThreshold))
	for _, info := range infos {
		ts := time.Now()
		err := b.kvclient.Set(info.Key, info.Val)
		if err != nil {
			fail++
			continue
		}
		elaspe := time.Since(ts)
		totalTime += elaspe
		success++
		for i := range timeDistribution {
			if elaspe < b.timeDistributionThreshold[i] {
				timeDistribution[i]++
			}
		}
	}

	return &KPI{"Set", success, fail, totalTime, 1, timeDistribution}
}

// BenchmarkGet benchmark for get
func (b *KVBenchmarker) BenchmarkGet(infos []*kvloader.KVInfo) *KPI {
	totalTime := time.Duration(0)
	success := 0
	fail := 0
	timeDistribution := make([]int, len(b.timeDistributionThreshold))
	for _, info := range infos {
		ts := time.Now()
		_, err := b.kvclient.Get(info.Key, info.Val)
		if err != nil {
			fail++
			continue
		}
		elaspe := time.Since(ts)
		totalTime += elaspe
		success++
		for i := range timeDistribution {
			if elaspe < b.timeDistributionThreshold[i] {
				timeDistribution[i]++
			}
		}
	}

	return &KPI{"Get", success, fail, totalTime, 1, timeDistribution}
}

// KPI key point index
type KPI struct {
	option           string
	success          int
	fail             int
	totalTime        time.Duration
	count            int
	timeDistribution []int
}

// Show KPI on console
func (k *KPI) Show() string {
	timeDistributionPercent := make([]string, len(k.timeDistribution))
	for i := range k.timeDistribution {
		timeDistributionPercent[i] = fmt.Sprintf("%.5f", float64(k.timeDistribution[i])/float64(k.fail+k.success))
	}

	return fmt.Sprintf(
		"%v\t%v\t%v\t% 8v\t% 8v\t% 8v\t%v\t%v",
		k.option, k.success, k.fail, k.totalTime,
		k.success*int(time.Second)*k.count/int(k.totalTime),
		k.totalTime/time.Duration(k.success),
		strings.Join(timeDistributionPercent, "\t"),
		float64(k.success)/float64(k.fail+k.success),
	)
}

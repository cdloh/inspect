package main

import (
	"flag"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/raintank/inspect/idx/cass"
	"gopkg.in/raintank/schema.v1"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type stat struct {
	def       schema.MetricDefinition
	firstSeen int64
}

var targets = make(map[string]stat)
var targetKeys = make([]string, 0)
var targetsLock sync.Mutex

var lag = metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
var numMetrics = metrics.NewGauge()
var nullPoints = metrics.NewCounter()

var env string
var cassAddr string
var cassKeyspace string
var carbonAddr string
var graphAddr string
var listenAddr string
var debug bool

func init() {
	flag.StringVar(&cassAddr, "cass-addr", "cassandra:9042", "cassandra address")
	flag.StringVar(&cassKeyspace, "cass-keyspace", "raintank", "cassandra keyspace to query")
	flag.StringVar(&env, "env", "", "environment for metrics")
	flag.StringVar(&carbonAddr, "carbon", "", "address to send metrics to")
	flag.StringVar(&graphAddr, "graphite", "", "graphite address")
	flag.StringVar(&listenAddr, "listen", ":6060", "http listener address.")
	flag.BoolVar(&debug, "debug", false, "debug mode")
}

func getMetrics(idx *cass.Cass) []schema.MetricDefinition {
	out := make([]schema.MetricDefinition, 0)
	met, err := idx.Get()
	perror(err)
	for _, m := range met {
		out = append(out, m)
	}
	return out
}

// for a metric to exist in the index at t=Y, there must at least have been 1 point for that metric
// at a time X where X < Y.  Hence, we can confidently say that if we see a metric at Y, we can
// demand data to show up for that metric at >=Y
// for our data check to be useful we need metrics to show up in cassandra soon after being in the pipeline,
// which is true because MT stores data to cassandra pretty much immediately
func updateMetrics(metrics []schema.MetricDefinition, seenAt int64) {
	numMetrics.Update(int64(len(metrics)))
	for _, met := range metrics {
		targetsLock.Lock()
		if _, ok := targets[met.Name]; !ok {
			targetKeys = append(targetKeys, met.Name)
			targets[met.Name] = stat{met, seenAt}
		}
		targetsLock.Unlock()
	}
}

func main() {
	flag.Parse()
	if env == "" {
		fmt.Fprintln(os.Stderr, "env must be set")
		os.Exit(2)
	}
	addr, _ := net.ResolveTCPAddr("tcp", carbonAddr)
	go metrics.Graphite(metrics.DefaultRegistry, 1e9, "graphite-watcher."+env+".", addr)
	go func() {
		log.Println("starting listener on", listenAddr)
		log.Printf("%s\n", http.ListenAndServe(listenAddr, nil))
	}()

	idx := cass.New(cassAddr, cassKeyspace)

	metrics.Register("lag", lag)
	metrics.Register("num_metrics", numMetrics)
	metrics.Register("null_points", nullPoints)

	args := flag.Args()
	if len(args) == 1 && args[0] == "one" {
		log.Println("mode: oneshot")
		metrics := getMetrics(idx)
		for len(metrics) == 0 {
			fmt.Println("waiting to see metrics in the index...")
			time.Sleep(4 * time.Second)
			metrics = getMetrics(idx)
		}
		updateMetrics(metrics, time.Now().Unix())
		targetsLock.Lock()
		key := targetKeys[rand.Intn(len(targets))]
		targetsLock.Unlock()
		test(time.Now().Unix(), targets[key], graphAddr, debug)
	} else {
		log.Println("mode: continuous")
		go func() {
			getEsTick := time.NewTicker(time.Second * time.Duration(1))
			for range getEsTick.C {
				updateMetrics(getMetrics(idx), time.Now().Unix())
			}
		}()

		tick := time.NewTicker(time.Millisecond * time.Duration(100))
		wg := &sync.WaitGroup{}
		for ts := range tick.C {
			targetsLock.Lock()
			if len(targetKeys) > 0 {
				key := targetKeys[rand.Intn(len(targets))]
				wg.Add(1)
				go func() {
					test(ts.Unix(), targets[key], graphAddr, debug)
					wg.Done()
				}()
			}
			targetsLock.Unlock()
		}
		wg.Wait()
	}
}

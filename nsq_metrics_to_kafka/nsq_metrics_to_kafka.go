package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/misc/app"
	"github.com/raintank/worldping-api/pkg/log"
	schemaV0 "gopkg.in/raintank/schema.v0"
	msgV0 "gopkg.in/raintank/schema.v0/msg"
	schemaV1 "gopkg.in/raintank/schema.v1"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	nsqTopic    = flag.String("nsq-topic", "metrics", "NSQ topic")
	nsqChannel  = flag.String("nsq-channel", "etl<random-number>#ephemeral", "NSQ channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	consumerOpts     = flag.String("nsq-consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	logLevel         = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	listenAddr       = flag.String("listen", ":6060", "http listener address.")

	concurrency    = flag.Int("concurrency", 10, "number of goroutines to consume messages from NSQ.")
	metricChanSize = flag.Int("metric-chan-size", 1000000, "size of buffered chan between consumer and publisher.")

	kafkaCompression = flag.String("kafka-comp", "none", "compression: none|gzip|snappy")
	kafkaBrokers     = flag.String("kafka-brokers", "", "kafka TCP addresses r e.g. localhost:9092 (may be be given multiple times as a comma-separated list)")
	kafkaTopic       = flag.String("kafka-topic", "metrics", "NSQ topic")
	kafkaBatchSize   = flag.Int("kafka-batch-size", 5000, "number of metrics in each batch sent to kafka")

	statsdAddr = flag.String("statsd-addr", "", "statsd address. e.g. localhost:8125")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	orgsFile = flag.String("orgs-list", "./orgs.txt", "path to file with list of orgs to migrate")

	flushDuration   met.Timer
	metricsInCount  met.Count
	messagesInCount met.Count
	metricsOutCount met.Count
	metricChanDepth met.Gauge

	orgList map[int]struct{}
	mu      sync.RWMutex
)

type MessageHandler struct {
	msgIn      msgV0.MetricData
	ch         chan *schemaV1.MetricData
	orderCheck map[string]int64
}

func NewMessageHandler(ch chan *schemaV1.MetricData) *MessageHandler {
	return &MessageHandler{
		msgIn:      msgV0.MetricData{Metrics: make([]*schemaV0.MetricData, 1)},
		ch:         ch,
		orderCheck: make(map[string]int64),
	}
}

func PublishMetrics(topic string, brokers []string, codec string, ch chan *schemaV1.MetricData, batchSize int) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = out.GetCompression(codec)
	err := config.Validate()
	if err != nil {
		log.Fatal(4, err.Error())
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	buf := make([]*sarama.ProducerMessage, 0, batchSize)
	dataBuf := make([][]byte, batchSize)
	keyBuf := make([][]byte, batchSize)
	for i := range keyBuf {
		keyBuf[i] = make([]byte, 8)
	}
	count := 0
	ticker := time.NewTicker(time.Second)
	var m *schemaV1.MetricData
	lastFlush := time.Now()
	log.Info("Kafka Publisher starting.")

	for {
		select {
		case <-ticker.C:
			metricChanDepth.Value(int64(len(ch)))
			if time.Since(lastFlush) >= time.Second {
				flush(client, buf)
				buf = buf[:0]
				count = 0
				lastFlush = time.Now()
			}
		case m = <-ch:

			if *logLevel < 2 {
				log.Debug(m.Id, m.Interval, m.Metric, m.Name, m.Time, m.Value, m.Tags)
			}

			data := dataBuf[count][:0]
			data, err := m.MarshalMsg(data)
			if err != nil {
				log.Error(3, err.Error())
				continue
			}

			// partition by organisation: metrics for the same org should go to the same
			// partition/MetricTank (optimize for locality~performance)
			// the extra 4B (now initialized with zeroes) is to later enable a smooth transition
			// to a more fine-grained partitioning scheme where
			// large organisations can go to several partitions instead of just one.
			key := keyBuf[count]
			binary.LittleEndian.PutUint32(key, uint32(m.OrgId))
			if *logLevel < 2 {
				log.Debug("key is %v", key)
			}
			buf = append(buf, &sarama.ProducerMessage{
				Key:   sarama.ByteEncoder(key),
				Topic: topic,
				Value: sarama.ByteEncoder(data),
			})
			count++
			if len(buf) == batchSize {
				flush(client, buf)
				buf = buf[:0]
				count = 0
				lastFlush = time.Now()
			}

		}
	}
	log.Info("Kafka Publisher ended.")
}

func flush(client sarama.SyncProducer, buf []*sarama.ProducerMessage) {
	preFlush := time.Now()
	err := client.SendMessages(buf)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		}
		return
	}
	flushDuration.Value(time.Since(preFlush))
	metricsOutCount.Inc(int64(len(buf)))
}

func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	err := h.msgIn.InitFromMsg(m.Body)
	if err != nil {
		log.Error(3, "%s: skipping message", err.Error())
		return nil
	}

	err = h.msgIn.DecodeMetricData()
	if err != nil {
		log.Error(3, "%s: skipping message", err.Error())
		return nil
	}
	messagesInCount.Inc(1)
	metricsInCount.Inc(int64(len(h.msgIn.Metrics)))

	for _, m := range h.msgIn.Metrics {
		if !shouldMigrate(m) {
			continue
		}
		if lastTs, ok := h.orderCheck[m.Id]; ok {
			if lastTs >= m.Time {
				log.Error(3, "%s out of order or duplicate. lastTs: %d  currentTs: %d", m.Id, lastTs, m.Time)
				continue
			}
		}
		h.orderCheck[m.Id] = m.Time

		newMetric := &schemaV1.MetricData{
			Name:     m.Name,
			Metric:   m.Metric,
			Interval: m.Interval,
			OrgId:    m.OrgId,
			Value:    m.Value,
			Time:     m.Time,
			Unit:     m.Unit,
			Mtype:    m.TargetType,
			Tags:     m.Tags,
		}
		newMetric.SetId()
		h.ch <- newMetric
	}

	return nil
}

func shouldMigrate(m *schemaV0.MetricData) bool {
	if strings.HasPrefix(m.Name, "litmus.") {
		mu.RLock()
		_, ok := orgList[m.OrgId]
		mu.RUnlock()
		if !ok {
			return false
		}
		m.Name = strings.Replace(m.Name, "litmus.", "worldping.", 1)
		m.Metric = strings.Replace(m.Metric, "litmus.", "worldping.", 1)
	}

	return true
}

func loadOrgs() {
	dat, err := ioutil.ReadFile(*orgsFile)
	if err != nil {
		log.Fatal(4, "couldn't read orgs-file. %s", err)
	}
	mu.Lock()
	for key := range orgList {
		delete(orgList, key)
	}
	for _, line := range strings.Split(string(dat), "\n") {
		org, err := strconv.ParseInt(strings.TrimSpace(line), 10, 64)
		if err != nil {
			log.Error(3, "invalid orgId: %s", line)
			continue
		}
		orgList[int(org)] = struct{}{}
	}
	mu.Unlock()
}

func main() {
	flag.Parse()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("nsq_metrics_to_stdout")
		return
	}

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*orgsFile); err != nil {
		log.Fatal(4, "orgs-file not found")
	}

	orgList = make(map[int]struct{})

	loadOrgs()
	go func() {
		last := time.Now()
		ticker := time.NewTicker(time.Minute)
		for range ticker.C {
			info, err := os.Stat(*orgsFile)
			if err != nil {
				log.Fatal(4, "failed to stat orgs-file. %s", err)
			}
			if info.ModTime().After(last) {
				loadOrgs()
			}
		}
	}()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "failed to lookup hostname. %s", err)
	}

	var stats met.Backend
	if *statsdAddr == "" {
		stats, err = helper.New(false, *statsdAddr, *statsdType, "nsq_metrics_to_kafka", strings.Replace(hostname, ".", "_", -1))
	} else {
		stats, err = helper.New(true, *statsdAddr, *statsdType, "nsq_metrics_to_kafka", strings.Replace(hostname, ".", "_", -1))
	}
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}
	flushDuration = stats.NewTimer("metricpublisher.out.flush_duration", 0)
	metricsInCount = stats.NewCount("metricpublisher.in.metrics")
	messagesInCount = stats.NewCount("metricpublisher.in.messages")
	metricsOutCount = stats.NewCount("metricpublisher.out.metrics")
	metricChanDepth = stats.NewGauge("metricpublisher.chan.size", 0)

	if *nsqChannel == "" || *nsqChannel == "etl<random-number>#ephemeral" {
		rand.Seed(time.Now().UnixNano())
		*nsqChannel = fmt.Sprintf("etl%06d#ephemeral", rand.Int()%999999)
	}

	if *nsqTopic == "" {
		log.Fatal(4, "--nsq-topic is required")
	}

	if *nsqdTCPAddrs == "" && *lookupdHTTPAddrs == "" {
		log.Fatal(4, "--nsqd-tcp-address or --lookupd-http-address required")
	}
	if *nsqdTCPAddrs != "" && *lookupdHTTPAddrs != "" {
		log.Fatal(4, "use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if *kafkaTopic == "" {
		log.Fatal(4, "--kafka-topic is required")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_to_kafka"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*nsqTopic, *nsqChannel, cfg)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	brokers := strings.Split(*kafkaBrokers, ",")

	metricChan := make(chan *schemaV1.MetricData, *metricChanSize)

	for i := 0; i < *concurrency; i++ {
		handler := NewMessageHandler(metricChan)
		consumer.AddHandler(handler)
	}

	go PublishMetrics(*kafkaTopic, brokers, *kafkaCompression, metricChan, *kafkaBatchSize)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	log.Info("connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	go func() {
		log.Info("starting listener for http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
			<-consumer.StopChan
		}
	}
}

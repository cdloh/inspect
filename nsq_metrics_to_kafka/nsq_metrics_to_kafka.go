package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/fakemetrics/out"
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

	kafkaCompression = flag.String("kafka-comp", "none", "compression: none|gzip|snappy")
	kafkaBrokers     = flag.String("kafka-brokers", "", "kafka TCP addresses r e.g. localhost:9092 (may be be given multiple times as a comma-separated list)")
	kafkaTopic       = flag.String("kafka-topic", "metrics", "NSQ topic")
)

type MessageHandler struct {
	msgIn  msgV0.MetricData
	Client sarama.SyncProducer
	Topic  string
}

func NewMessageHandler(topic string, brokers []string, codec string) (*MessageHandler, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = out.GetCompression(codec)
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &MessageHandler{
		msgIn:  msgV0.MetricData{Metrics: make([]*schemaV0.MetricData, 1)},
		Client: client,
		Topic:  topic,
	}, nil
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

	var data []byte
	newMetric := &schemaV1.MetricData{}
	payload := make([]*sarama.ProducerMessage, len(h.msgIn.Metrics))

	for i, m := range h.msgIn.Metrics {

		*newMetric = schemaV1.MetricData{
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
		data, err := newMetric.MarshalMsg(data)
		if err != nil {
			return err
		}

		fmt.Println(newMetric.Id, m.Id, m.Interval, newMetric.Metric, m.Name, m.Time, m.Value, m.Tags)

		// partition by organisation: metrics for the same org should go to the same
		// partition/MetricTank (optimize for locality~performance)
		// the extra 4B (now initialized with zeroes) is to later enable a smooth transition
		// to a more fine-grained partitioning scheme where
		// large organisations can go to several partitions instead of just one.
		key := make([]byte, 8)
		binary.LittleEndian.PutUint32(key, uint32(m.OrgId))
		payload[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(key),
			Topic: h.Topic,
			Value: sarama.ByteEncoder(data),
		}
	}
	err = h.Client.SendMessages(payload)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		}
		return err
	}

	return nil
}

func main() {
	flag.Parse()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("nsq_metrics_to_stdout")
		return
	}

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
	err := app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*nsqTopic, *nsqChannel, cfg)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	brokers := strings.Split(*kafkaBrokers, ",")
	handler, err := NewMessageHandler(*kafkaTopic, brokers, *kafkaCompression)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	consumer.AddHandler(handler)

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

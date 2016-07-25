package main

import (
	"fmt"

	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/met/helper"
	schemaV0 "gopkg.in/raintank/schema.v0"
	msgV0 "gopkg.in/raintank/schema.v0/msg"
	schemaV1 "gopkg.in/raintank/schema.v1"
)

func generateMessages(interval, orgOffset, orgs, keysPerOrg int, ts int64) ([]*nsq.Message, error) {
	metrics := make([]*schemaV0.MetricData, orgs*keysPerOrg)
	i := 0
	for o := 1; o <= orgs; o++ {
		for k := 1; k <= keysPerOrg; k++ {
			metrics[i] = &schemaV0.MetricData{
				Name:       fmt.Sprintf("litmus.%d.of.a.metric.%d", orgOffset, k),
				Metric:     "litmus.id.of.a.metric",
				OrgId:      orgOffset + o,
				Interval:   interval,
				Value:      0,
				Unit:       "ms",
				Time:       ts,
				TargetType: "gauge",
				Tags:       []string{"some_tag", "ok", fmt.Sprintf("id:%d", k)},
			}
			metrics[i].SetId()
			i++
		}
	}
	subslices := schemaV0.Reslice(metrics, 50)
	messages := make([]*nsq.Message, len(subslices))
	for i, subslice := range subslices {
		id := time.Now().UnixNano()
		data, err := msgV0.CreateMsg(subslice, id, msgV0.FormatMetricDataArrayMsgp)
		if err != nil {
			return nil, err
		}
		messages[i] = &nsq.Message{
			Body: data,
		}
	}
	return messages, nil
}

func TestHandleMessage(t *testing.T) {
	orgList = make(map[int]struct{})
	for i := 1; i <= 5; i++ {
		orgList[i] = struct{}{}
		orgList[50+i] = struct{}{}
	}

	stats, err := helper.New(false, "localhost", "standard", "nsq_metrics_to_kafka", "default")

	if err != nil {
		t.Fatal("failed to initialize statsd. %s", err)
	}
	flushDuration = stats.NewTimer("metricpublisher.out.flush_duration", 0)
	metricsInCount = stats.NewCount("metricpublisher.in.metrics")
	messagesInCount = stats.NewCount("metricpublisher.in.messages")
	metricsOutCount = stats.NewCount("metricpublisher.out.metrics")
	metricChanDepth = stats.NewGauge("metricpublisher.chan.size", 0)

	metricChan := make(chan *schemaV1.MetricData, 1000)

	recvDone := make(chan struct{})

	recvCount := 0
	go func() {
		fmt.Println("reading from metricChan")
		orderCheck := make(map[string]int64)
		for m := range metricChan {
			if lastTs, ok := orderCheck[m.Id]; ok {
				if lastTs >= m.Time {
					t.Errorf("%s out of order or duplicate. lastTs: %d  currentTs: %d", m.Id, lastTs, m.Time)
				}
			}
			orderCheck[m.Id] = m.Time
			recvCount++
		}
		close(recvDone)
	}()

	handler := NewMessageHandler(metricChan)
	sentCount := 0

	ts := time.Now().Unix()
	for i := 0; i < 10; i++ {
		msgs, err := generateMessages(1, (i%2)*50, 10, 100, ts+int64(i))
		if err != nil {
			t.Fatal(err)
		}
		for _, msg := range msgs {
			err = handler.HandleMessage(msg)
			if err != nil {
				t.Fatal(err)
			}
		}
		sentCount += 500
	}

	close(metricChan)

	<-recvDone
	if recvCount != sentCount {
		t.Fatal("didnt process all metrics")
	}
}

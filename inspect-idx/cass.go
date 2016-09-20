package main

import (
	"strings"

	"github.com/gocql/gocql"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type CasIdx struct {
	keyspace string
	hosts    []string
	cluster  *gocql.ClusterConfig
	session  *gocql.Session
}

func New(hostStr, keyspace string) *CasIdx {
	hosts := strings.Split(hostStr, ",")

	return &CasIdx{
		keyspace: keyspace,
		hosts:    hosts,
		cluster:  gocql.NewCluster(hosts...),
	}
}

func (c *CasIdx) Get() ([]schema.MetricDefinition, error) {
	c.cluster.Keyspace = c.keyspace
	c.cluster.ProtoVersion = 4
	session, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "IDX-C failed to create cassandra session. %s", err)
		return nil, err
	}

	c.session = session
	defs := make([]schema.MetricDefinition, 0)
	iter := c.session.Query("SELECT def from metric_def_idx").Iter()

	var data []byte
	mdef := schema.MetricDefinition{}
	for iter.Scan(&data) {
		_, err := mdef.UnmarshalMsg(data)
		if err != nil {
			log.Error(3, "IDX-C Bad definition in index. %s - %s", data, err)
			continue
		}
		defs = append(defs, mdef)
	}
	return defs, nil
}

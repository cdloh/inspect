package main

import (
	"flag"
	"fmt"
	"github.com/raintank/dur"
	"github.com/raintank/metrictank/metricdef"
	"gopkg.in/raintank/schema.v1"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var esAddr = flag.String("es-addr", "localhost:9200", "elasticsearch address")
var esIndex = flag.String("es-index", "metric", "elasticsearch index to query")
var format = flag.String("format", "list", "format: list|vegeta-graphite|vegeta-mt|vegeta-mt-graphite")
var maxAge = flag.Int("max-age", 23400, "max age (last update diff with now) of metricdefs. defaults to 6.5hr. use 0 to disable")
var from = flag.String("from", "30min", "from. eg '30min', '5h', '14d', etc")
var silent = flag.Bool("silent", false, "silent mode (don't print number of metrics loaded to stderr)")
var fromS uint32
var total int

func main() {
	flag.Parse()
	var show func(ds []*schema.MetricDefinition)
	switch *format {
	case "list":
		show = showList
	case "vegeta-graphite":
		show = showVegetaGraphite
	case "vegeta-mt":
		show = showVegetaMT
	case "vegeta-mt-graphite":
		show = showVegetaMTGraphite
	default:
		log.Fatal("invalid format")
	}
	var err error
	fromS, err = dur.ParseUNsec(*from)
	perror(err)
	defs, err := metricdef.NewDefsEs(*esAddr, "", "", *esIndex, nil)
	perror(err)
	met, scroll_id, err := defs.GetMetrics("")
	perror(err)
	show(met)
	for scroll_id != "" {
		met, scroll_id, err = defs.GetMetrics(scroll_id)
		perror(err)
		show(met)
	}
	if !*silent {
		fmt.Fprintf(os.Stderr, "listed %d metrics\n", total)
	}
}

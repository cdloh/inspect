package main

import (
	"flag"
	"fmt"
	"github.com/raintank/dur"
	"gopkg.in/raintank/schema.v1"
	"log"
	"os"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

//var esAddr = flag.String("es-addr", "localhost:9200", "elasticsearch address")
//var esIndex = flag.String("es-index", "metric", "elasticsearch index to query")
var maxAge = flag.Int("max-age", 23400, "max age (last update diff with now) of metricdefs. defaults to 6.5hr. use 0 to disable")
var from = flag.String("from", "30min", "from. eg '30min', '5h', '14d', etc")
var silent = flag.Bool("silent", false, "silent mode (don't print number of metrics loaded to stderr)")
var fromS uint32
var total int

func main() {
	flag.Usage = func() {
		fmt.Printf("%s by Dieter_be\n", os.Args[0])
		fmt.Println("Usage:")
		fmt.Printf("  inspect-idx [flags] idxtype host keyspace/index output \n")
		fmt.Printf("  idxtype cass: \n")
		fmt.Printf("    host: comma separated list of cassandra addresses in host:port form\n")
		fmt.Printf("    keyspace: cassandra keyspace\n")
		fmt.Printf("  idxtype es: not supported at this point\n")
		fmt.Printf("  output: list|vegeta-graphite|vegeta-mt|vegeta-mt-graphite\n")
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 4 {
		flag.Usage()
		os.Exit(-1)
	}
	args := flag.Args()
	var show func(ds []schema.MetricDefinition)

	switch args[3] {
	case "list":
		show = showList
	case "vegeta-graphite":
		show = showVegetaGraphite
	case "vegeta-mt":
		show = showVegetaMT
	case "vegeta-mt-graphite":
		show = showVegetaMTGraphite
	default:
		log.Fatal("invalid output")
	}
	var err error
	fromS, err = dur.ParseUNsec(*from)
	perror(err)

	if args[0] != "cass" {
		fmt.Fprintf(os.Stderr, "only cass supported at this point")
		flag.Usage()
		os.Exit(-1)
	}

	cass := New(args[1], args[2])

	defs, err := cass.Get()
	perror(err)
	show(defs)

	if !*silent {
		fmt.Fprintf(os.Stderr, "listed %d metrics\n", total)
	}
}

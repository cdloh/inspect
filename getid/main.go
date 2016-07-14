package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/raintank/schema.v0"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: getid <orgid> <name> [tag1, [tag2, ...]] # tag can be a word or key:val")
}

func main() {
	if len(os.Args) >= 2 && (os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help") {
		usage()
		os.Exit(0)
	}
	if len(os.Args) < 3 {
		usage()
		os.Exit(2)
	}
	orgId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		usage()
		os.Exit(2)
	}
	if os.Args[2] == "" {
		usage()
		os.Exit(2)
	}
	md := &schema.MetricDefinition{
		OrgId: orgId,
		Name:  os.Args[2],
		Tags:  os.Args[3:],
	}
	md.SetId()
	spew.Dump(md)
}

package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"gopkg.in/raintank/schema.v1"
)

func showList(ds []schema.MetricDefinition) {
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			fmt.Println(d.OrgId, d.Name)
		}
	}
}
func showVegetaGraphite(ds []schema.MetricDefinition) {
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			fmt.Printf("GET http://localhost:8888/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", d.Name, *from, d.OrgId)
		}
	}
}

func showVegetaMT(ds []schema.MetricDefinition) {
	from := time.Now().Add(-time.Duration(fromS) * time.Second)
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			fmt.Printf("GET http://localhost:6063/get?target=%s&from=%d\n", d.Id, from.Unix())
		}
	}
}

func showVegetaMTGraphite(ds []schema.MetricDefinition) {
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			mode := rand.Intn(3)
			name := d.Name
			if mode == 0 {
				// in this mode, replaces a node with a dot
				which := rand.Intn(d.NodeCount)
				parts := strings.Split(d.Name, ".")
				parts[which] = "*"
				name = strings.Join(parts, ".")
			} else if mode == 1 {
				// randomly replace chars with a *
				// note that in 1/5 cases, nothing happens
				// and otherwise, sometimes valid patterns are produced,
				// but it's also possible to produce patterns that won't match anything (if '.' was taken out)
				chars := rand.Intn(5)
				pos := rand.Intn(len(d.Name) - chars)
				name = name[0:pos] + "*" + name[pos+chars:]
			}
			// mode 3: do nothing :)

			fmt.Printf("GET http://localhost:6063/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", name, *from, d.OrgId)
		}
	}
}

/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metricdef

import (
	"fmt"
	"strings"

	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type ResultCallback func(id string, ok bool)

type DefsEs struct {
	index string
	*elastigo.Conn
	*elastigo.BulkIndexer
	cb ResultCallback
}

// cb can be nil now, as long as it's set by the time you start indexing.
func NewDefsEs(addr, user, pass, indexName string, cb ResultCallback) (*DefsEs, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid tcp addr %q", addr)
	}

	d := &DefsEs{
		indexName,
		elastigo.NewConn(),
		nil,
		cb,
	}

	d.Conn.Domain = parts[0]
	d.Conn.Port = parts[1]
	if user != "" && pass != "" {
		d.Conn.Username = user
		d.Conn.Password = pass
	}
	if exists, err := d.ExistsIndex(indexName, "", nil); err != nil && err.Error() != "record not found" {
		return nil, err
	} else {
		if !exists {
			log.Info("ES: initializing %s Index with mapping", indexName)
			//lets apply the mapping.
			metricMapping := `{
				"mappings": {
		            "_default_": {
		                "dynamic_templates": [
		                    {
		                        "strings": {
		                            "mapping": {
		                                "index": "not_analyzed",
		                                "type": "string"
		                            },
		                            "match_mapping_type": "string"
		                        }
		                    }
		                ],
		                "_all": {
		                    "enabled": false
		                },
		                "properties": {}
		            },
		            "metric_index": {
		                "dynamic_templates": [
		                    {
		                        "strings": {
		                            "mapping": {
		                                "index": "not_analyzed",
		                                "type": "string"
		                            },
		                            "match_mapping_type": "string"
		                        }
		                    }
		                ],
		                "_all": {
		                    "enabled": false
		                },
		                "_timestamp": {
		                    "enabled": false
		                },
		                "properties": {
		                    "id": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "interval": {
		                        "type": "long"
		                    },
		                    "lastUpdate": {
		                        "type": "long"
		                    },
		                    "metric": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "name": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "node_count": {
		                        "type": "long"
		                    },
		                    "org_id": {
		                        "type": "long"
		                    },
		                    "tags": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "mtype": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "unit": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    }
		                }
					}
				}
			}`

			_, err = d.DoCommand("PUT", fmt.Sprintf("/%s", indexName), nil, metricMapping)
			if err != nil {
				return nil, err
			}
		}
	}

	return d, nil
}

// if scroll_id specified, will resume that scroll session.
// returns scroll_id if there's any more metrics to be fetched.
func (d *DefsEs) GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error) {
	// future optimiz: clear scroll when finished, tweak length of items, order by _doc
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
	defs := make([]*schema.MetricDefinition, 0)
	var err error
	var out elastigo.SearchResult
	if scroll_id == "" {
		out, err = d.Search(d.index, "metric_index", map[string]interface{}{"scroll": "1m", "size": 1000}, nil)
	} else {
		out, err = d.Scroll(map[string]interface{}{"scroll": "1m"}, scroll_id)
	}
	if err != nil {
		return defs, "", err
	}
	for _, h := range out.Hits.Hits {
		mdef, err := schema.MetricDefinitionFromJSON(*h.Source)
		if err != nil {
			return defs, "", err
		}
		defs = append(defs, mdef)
	}
	scroll_id = ""
	if out.Hits.Len() > 0 {
		scroll_id = out.ScrollId
	}

	return defs, scroll_id, nil
}

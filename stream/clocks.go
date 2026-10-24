package stream

import (
	"encoding/base64"
	"encoding/binary"
	"sort"

	"github.com/SoraDaibu/kstream-go/topology"
)

// Connected components describe clock ownership, not worker or goroutine
// boundaries. Cut only the broker edge into each repartition source. Table
// lookup edges and merged sources deliberately share a processing clock.
func processingScopes(plan topology.Plan, processors []string) map[string]string {
	if len(processors) == 0 {
		return nil
	}
	nodes := plan.Nodes()
	parent := make(map[string]string, len(nodes))
	var root func(string) string
	root = func(name string) string {
		if parent[name] != name {
			parent[name] = root(parent[name])
		}
		return parent[name]
	}
	for _, n := range nodes {
		parent[n.Name] = n.Name
		if n.Kind != "repartition-source" {
			for _, p := range n.Parents {
				parent[root(n.Name)] = root(p)
			}
		}
	}
	sources := make(map[string][]string)
	for _, n := range nodes {
		if n.Kind == "source" || n.Kind == "repartition-source" {
			id := root(n.Name)
			sources[id] = append(sources[id], n.Name)
		}
	}
	// The common single-stage path already has the correct native task clock.
	if len(sources) < 2 {
		return nil
	}
	needed := make(map[string]string)
	for _, p := range processors {
		id := root(p)
		names := sources[id]
		sort.Strings(names)
		var encoded []byte
		for _, name := range names {
			encoded = binary.AppendUvarint(encoded, uint64(len(name)))
			encoded = append(encoded, name...)
		}
		// Encode arbitrary Go string bytes injectively, including separators or
		// invalid UTF-8. JSON checkpoint keys must never normalize scope IDs.
		needed[id] = "v1:" + base64.RawURLEncoding.EncodeToString(encoded)
	}
	scopes := make(map[string]string)
	for _, n := range nodes {
		if scope := needed[root(n.Name)]; scope != "" {
			scopes[n.Name] = scope
		}
	}
	return scopes
}

package stream

import (
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/topology"
)

func TestClockComponentsIncludeTableEdgesAndHaveStableDistinctNames(t *testing.T) {
	compile := func(reverse bool) map[string]string {
		sources := []topology.Node{{Name: "events", Kind: "source"}, {Name: "table", Kind: "source"}}
		if reverse {
			sources[0], sources[1] = sources[1], sources[0]
		}
		nodes := append(sources,
			topology.Node{Name: "join", Kind: "stream-table-join", Parents: []string{"events", "table"}},
			topology.Node{Name: "before", Kind: "processor", Parents: []string{"join"}},
			topology.Node{Name: "exchange", Kind: "repartition", Parents: []string{"before"}},
			topology.Node{Name: "exchange-source", Kind: "repartition-source", Parents: []string{"exchange"}},
			topology.Node{Name: "after", Kind: "processor", Parents: []string{"exchange-source"}},
			topology.Node{Name: "\xff", Kind: "source"},
			topology.Node{Name: "byte-one", Kind: "processor", Parents: []string{"\xff"}},
			topology.Node{Name: "\xfe", Kind: "source"},
			topology.Node{Name: "byte-two", Kind: "processor", Parents: []string{"\xfe"}},
		)
		plan, err := topology.New(nodes)
		if err != nil {
			t.Fatal(err)
		}
		return processingScopes(plan, []string{"before", "after", "byte-one", "byte-two"})
	}
	a, b := compile(false), compile(true)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("source construction order changed clock identity", a, b)
	}
	if a["before"] == "" || a["table"] != a["events"] || a["table"] != a["before"] || a["after"] == a["before"] || a["exchange-source"] != a["after"] {
		t.Fatal("clock graph ignored a table dependency or crossed broker delivery", a)
	}
	if a["byte-one"] == a["byte-two"] {
		t.Fatal("scope identity normalized distinct source bytes", a)
	}
}

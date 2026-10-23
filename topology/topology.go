// Package topology models a logical plan independently of execution factories.
package topology

import (
 "fmt"
 "strings"
)

type Node struct {
 Name string
 Kind string
 Parents []string
 Store string
}
type Plan struct { nodes []Node }
func New(nodes []Node)(Plan,error) {
 if len(nodes)==0 { return Plan{},fmt.Errorf("empty topology") }
 seen:=make(map[string]bool);stores:=make(map[string]bool)
 copyNodes:=make([]Node,len(nodes))
 for i,n:=range nodes {
  if n.Name=="" || seen[n.Name] {return Plan{},fmt.Errorf("empty or duplicate node name %q",n.Name)}
  for _,p:=range n.Parents {if !seen[p] {return Plan{},fmt.Errorf("unknown parent %q",p)}}
  if n.Kind=="repartition" {return Plan{},fmt.Errorf("repartition %q requires an unimplemented Kafka exchange adapter",n.Name)}
  if n.Store!="" {if stores[n.Store] {return Plan{},fmt.Errorf("duplicate store %q",n.Store)};stores[n.Store]=true}
  seen[n.Name]=true;copyNodes[i]=n;copyNodes[i].Parents=append([]string(nil),n.Parents...)
 }
 return Plan{nodes:copyNodes},nil
}
func (p Plan) Nodes() []Node { out:=make([]Node,len(p.nodes));for i,n:=range p.nodes {out[i]=n;out[i].Parents=append([]string(nil),n.Parents...)};return out }
func (p Plan) Describe() string {
 var b strings.Builder
 for _,n:=range p.nodes {fmt.Fprintf(&b,"%s %s <- %s",n.Name,n.Kind,strings.Join(n.Parents,","));if n.Store!=""{fmt.Fprintf(&b," store=%s",n.Store)};b.WriteByte('\n')};return b.String()
}

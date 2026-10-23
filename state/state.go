// Package state implements an owner-local state image. It has no locks: callers
// must serialize access through their task. Backends own durable synchronization.
package state

import "github.com/SoraDaibu/kstream-go/record"

type Mutation struct { Store, Key string; Value []byte }
type Image map[string]map[string][]byte
func (s Image) Apply(m Mutation) {
 if m.Value==nil { delete(s[m.Store],m.Key); return }
 if s[m.Store]==nil { s[m.Store]=make(map[string][]byte) }
 s[m.Store][m.Key]=record.CloneBytes(m.Value)
}
func (s Image) Clone() Image {
 c:=make(Image);for name,entries:=range s { c[name]=make(map[string][]byte);for k,v:=range entries {c[name][k]=record.CloneBytes(v)} };return c
}

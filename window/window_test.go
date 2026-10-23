package window_test

import (
 "math"
 "testing"
 "github.com/SoraDaibu/kstream-go/window"
)
func TestBoundsAndGrace(t *testing.T){
 w:=window.Tumbling{Size:10,Grace:5}
 for _,tt:=range []struct{timestamp,start,end int64}{{0,0,10},{9,0,10},{10,10,20}}{s,e,err:=w.Bounds(tt.timestamp);if err!=nil||s!=tt.start||e!=tt.end{t.Fatalf("%+v: %d %d %v",tt,s,e,err)}}
 if w.Closed(10,14)||!w.Closed(10,15){t.Fatal("incorrect grace boundary")}
 if _,_,err:=w.Bounds(-1);err==nil{t.Fatal("negative timestamp accepted")}
 if _,_,err:=w.Bounds(math.MaxInt64);err==nil{t.Fatal("overflow accepted")}
 if w.Closed(math.MaxInt64,math.MaxInt64){t.Fatal("grace overflow closed window")}
}

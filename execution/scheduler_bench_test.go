package execution_test

import (
 "context"
 "fmt"
 "runtime"
 "testing"

 "github.com/SoraDaibu/kstream-go/execution"
 "github.com/SoraDaibu/kstream-go/record"
 "github.com/SoraDaibu/kstream-go/state"
)
// A task-private backend removes the shared broker-model mutex from the
// scheduling comparison. It deliberately does not model durability or EOS.
type benchBackend struct { next int64 }
func (b *benchBackend) Open(context.Context,execution.Spec)(uint64,execution.Snapshot,error){return 1,execution.Snapshot{State:make(state.Image),StreamTime:-1},nil}
func (b *benchBackend) Commit(_ context.Context,_ execution.Spec,_ uint64,tx execution.Transaction)error{b.next=tx.NextOffsets["in"];return nil}

func BenchmarkSchedule(b *testing.B){
 workerCounts:=[]int{1};if cpus:=runtime.GOMAXPROCS(0);cpus>1{workerCounts=append(workerCounts,cpus)}
 for _,tasks:=range []int{8,256}{for _,records:=range []int{1,256}{for _,work:=range []int{0,128}{for _,workers:=range workerCounts{for _,strategy:=range []execution.Strategy{execution.BoundedWorkers,execution.GoroutinePerTask}{
  if strategy==execution.GoroutinePerTask&&workers!=1{continue}
  name:=fmt.Sprintf("tasks=%d/records=%d/work=%d/workers=%d/strategy=%d",tasks,records,work,workers,strategy)
  b.Run(name,func(b *testing.B){
   lanes:=make([]execution.Lane,tasks);backends:=make([]*benchBackend,tasks)
   for i:=range lanes{backends[i]=&benchBackend{};task,e:=execution.New(ctx,execution.Spec{ApplicationID:"bench",ID:fmt.Sprint(i)},backends[i]);if e!=nil{b.Fatal(e)}
    if e=task.Register("in",func(r record.Bytes)error{v:=uint64(r.Offset);for j:=0;j<work;j++{v=v*6364136223846793005+1};runtime.KeepAlive(v);return nil});e!=nil{b.Fatal(e)}
    lanes[i]=execution.Lane{Task:task,Records:make([]record.Bytes,records)}
   }
   b.ReportAllocs();b.ResetTimer()
   for n:=0;n<b.N;n++{
    for i:=range lanes{for j:=range lanes[i].Records{lanes[i].Records[j]=record.Bytes{Topic:"in",Offset:backends[i].next+int64(j)}}}
    if e:=execution.Run(ctx,lanes,workers,strategy);e!=nil{b.Fatal(e)}
   }
   b.ReportMetric(float64(tasks*records),"records/op")
  })
 }}}}}
}

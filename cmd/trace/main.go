// trace executes the same finite input trace as the independent Java oracle.
package main

import (
 "bufio"
 "context"
 "fmt"
 "log"
 "os"
 "strconv"
 "strings"

 "github.com/SoraDaibu/kstream-go/adapters/memory"
 "github.com/SoraDaibu/kstream-go/execution"
 "github.com/SoraDaibu/kstream-go/record"
 "github.com/SoraDaibu/kstream-go/serde"
 "github.com/SoraDaibu/kstream-go/stream"
 "github.com/SoraDaibu/kstream-go/window"
)
func nullable(v string)[]byte{if v=="~"{return nil};b:=make([]byte,len(v));copy(b,v);return b}
func field(v []byte)string{if v==nil{return "~"};return string(v)}
func run()error{
 if len(os.Args)!=3{return fmt.Errorf("usage: trace scenario input.tsv")};scenario:=os.Args[1]
 b:=stream.NewStreamsBuilder()
 var table stream.KTable[string,string]
 if scenario=="join"||scenario=="table"{table=stream.Table(b,"table","lookup",serde.String,serde.String)}
 if scenario=="table"{table.ToStream().To("out")}else{
  s:=stream.Stream(b,"in",serde.String,serde.String)
  switch scenario {
  case "filter":s.Filter("nonempty",func(r record.Record[string,string])bool{return !r.NullValue&&r.Value!=""}).To("out")
  case "count":stream.Map(stream.Count(s,"counts").ToStream(),"format",serde.String,serde.String,func(r record.Record[string,int64])record.Record[string,string]{return record.Record[string,string]{Key:r.Key,Value:strconv.FormatInt(r.Value,10),Timestamp:r.Timestamp}}).To("out")
  case "join":stream.Join(s,table,"join",serde.String,func(a,c string)string{return a+":"+c}).To("out")
  case "window":stream.Map(stream.WindowedCount(s,"windows",window.Tumbling{Size:10,Grace:5}),"format-window",serde.String,serde.String,func(r record.Record[window.Key[string],int64])record.Record[string,string]{return record.Record[string,string]{Key:fmt.Sprintf("%s@%d:%d",r.Key.Key,r.Key.Start,r.Key.End),Value:strconv.FormatInt(r.Value,10),Timestamp:r.Timestamp}}).To("out")
  default:return fmt.Errorf("unknown scenario %q",scenario)
  }
 }
 p,err:=b.Build();if err!=nil{return err};backend:=memory.New();id:=execution.Spec{ApplicationID:"oracle",ID:"0"};ctx:=context.Background();task,err:=p.NewTask(ctx,id,backend);if err!=nil{return err}
 file,err:=os.Open(os.Args[2]);if err!=nil{return err};defer file.Close();scanner:=bufio.NewScanner(file);offsets:=make(map[string]int64)
 for scanner.Scan(){line:=scanner.Text();if line==""||strings.HasPrefix(line,"#"){continue};f:=strings.Split(line,"\t");if len(f)!=4{return fmt.Errorf("trace requires four fields")};ts,err:=strconv.ParseInt(f[1],10,64);if err!=nil{return err}
  err=task.Process(ctx,record.Bytes{Topic:f[0],Offset:offsets[f[0]],Timestamp:ts,Key:nullable(f[2]),Value:nullable(f[3])});if err!=nil{return err};offsets[f[0]]++
 };if err:=scanner.Err();err!=nil{return err}
 for _,r:=range backend.Outputs(id){fmt.Printf("%s\t%s\t%s\t%d\n",r.Topic,field(r.Key),field(r.Value),r.Timestamp)};return nil
}
func main(){if err:=run();err!=nil{log.Fatal(err)}}

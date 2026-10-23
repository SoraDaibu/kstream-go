// Package window defines event-time window boundaries in Unix milliseconds.
package window

import (
 "errors"
 "math"
)

type Tumbling struct { Size, Grace int64 }
func (w Tumbling) Validate()error {if w.Size<=0||w.Grace<0{return errors.New("positive window size and nonnegative grace required")};return nil}
func (w Tumbling) Bounds(timestamp int64)(int64,int64,error) {
 if err:=w.Validate();err!=nil{return 0,0,err};if timestamp<0{return 0,0,errors.New("negative timestamp")}
 start:=timestamp-timestamp%w.Size;if start>math.MaxInt64-w.Size{return 0,0,errors.New("window end overflow")};return start,start+w.Size,nil
}
func (w Tumbling) Closed(end,streamTime int64)bool {if end>math.MaxInt64-w.Grace{return false};return streamTime>=end+w.Grace}
type Key[K any] struct { Key K; Start,End int64 }

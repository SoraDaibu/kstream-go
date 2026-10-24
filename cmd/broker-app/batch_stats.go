package main

import "github.com/SoraDaibu/kstream-go/adapters/kafka"

type batchStats struct {
	Batches      int64         `json:"batches"`
	Records      int64         `json:"records"`
	Tasks        int64         `json:"tasks"`
	MinRecords   int           `json:"min_records"`
	MaxRecords   int           `json:"max_records"`
	ProcessNanos int64         `json:"process_nanos"`
	Sizes        map[int]int64 `json:"size_upper_bounds"`
}

func (s *batchStats) add(metrics kafka.GroupBatchMetrics) {
	s.Batches++
	s.Records += int64(metrics.Records)
	s.Tasks += int64(metrics.Tasks)
	s.ProcessNanos += metrics.ProcessDuration.Nanoseconds()
	if s.MinRecords == 0 || metrics.Records < s.MinRecords {
		s.MinRecords = metrics.Records
	}
	if metrics.Records > s.MaxRecords {
		s.MaxRecords = metrics.Records
	}
	if s.Sizes == nil {
		s.Sizes = make(map[int]int64)
	}
	upper := 1
	for upper < metrics.Records {
		upper *= 2
	}
	s.Sizes[upper]++
}

type batchObservation struct {
	Event             string     `json:"event"`
	Warmup            batchStats `json:"warmup"`
	Measurement       batchStats `json:"measurement"`
	BoundaryCrossed   bool       `json:"boundary_crossed"`
	seen, warmupLimit int64
}

func (o *batchObservation) add(metrics kafka.GroupBatchMetrics) {
	before := o.seen
	o.seen += int64(metrics.Records)
	if before < o.warmupLimit {
		if o.seen > o.warmupLimit {
			o.BoundaryCrossed = true
		}
		o.Warmup.add(metrics)
	} else {
		o.Measurement.add(metrics)
	}
}

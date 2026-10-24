package main

import (
	"errors"
	"os"
	"runtime"
	"runtime/pprof"
)

// Profiles cover the whole diagnostic process, including startup/warmup and
// shutdown. The heap profile is taken after engine cleanup and an explicit GC;
// cumulative alloc_space is useful, inuse_space is not peak memory.
func startProfiles(cpuPath, heapPath string) (func() error, error) {
	if cpuPath != "" && cpuPath == heapPath {
		return nil, errors.New("CPU and heap profiles require distinct paths")
	}
	var cpu, heap *os.File
	var err error
	if heapPath != "" {
		heap, err = os.Create(heapPath)
		if err != nil {
			return nil, err
		}
	}
	if cpuPath != "" {
		cpu, err = os.Create(cpuPath)
		if err == nil {
			err = pprof.StartCPUProfile(cpu)
		}
		if err != nil {
			if cpu != nil {
				err = errors.Join(err, cpu.Close())
			}
			if heap != nil {
				err = errors.Join(err, heap.Close())
			}
			return nil, err
		}
	}
	return func() (err error) {
		if cpu != nil {
			pprof.StopCPUProfile()
			err = cpu.Close()
		}
		if heap != nil {
			runtime.GC()
			err = errors.Join(err, pprof.WriteHeapProfile(heap), heap.Close())
		}
		return err
	}, nil
}

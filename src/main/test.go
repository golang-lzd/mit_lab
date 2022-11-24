package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	timer := time.NewTimer(3 * time.Second)
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		select {
		case <-timer.C:
			fmt.Println("done", time.Since(start).Seconds())
			wg.Done()
		}
	}()
	time.Sleep(2 * time.Second)
	timer.Stop()
	timer.Reset(3 * time.Second)
	wg.Wait()
}

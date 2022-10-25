package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	f, err := os.OpenFile("log_coordinator.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer f.Close()

	log.SetOutput(f)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	log.Println("开始接受 worker 的请求")
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)

}

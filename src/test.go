package main

import (
	"fmt"
	"time"
)

type Stu struct {
	Var []int
}

func main() {
	data := []int{1, 2, 3, 4}

	go func(_data []int) {
		time.Sleep(time.Second)
		_data = _data[1:]
	}(data)
	go func(_data []int) {
		args := Stu{Var: data[:2]}
		time.Sleep(2 * time.Second)
		fmt.Println(args)
	}(data)
}

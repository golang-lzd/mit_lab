package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
)

type A struct {
	Data []*B
}

type B struct {
	Val  int
	Name string
}

func main() {
	tmp := []*B{}
	for i := 0; i < 100; i++ {
		tmp = append(tmp, &B{
			Val:  rand.Intn(100000),
			Name: strconv.FormatInt(rand.Int63n(100000), 10),
		})
	}
	//a := A{
	//	Data: tmp,
	//}
	//t := a.Data
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Val > tmp[i].Val
	})

	for i := 0; i < len(tmp); i++ {
		fmt.Println(tmp[i])
	}
}

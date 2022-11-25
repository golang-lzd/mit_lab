package shardctrler

import (
	"fmt"
	"testing"
)

func TestShardCtrler_AdjustConfig(t *testing.T) {
	arr := [10]int{1, 2, 3}
	data := arr
	fmt.Println(arr)
	fmt.Println(data)
	arr[0] = 10
	fmt.Println(arr)
	fmt.Println(data)
}

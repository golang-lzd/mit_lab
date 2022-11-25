package shardctrler

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestShardCtrler_AdjustConfig1(t *testing.T) {
	shardCtler := ShardCtrler{}
	config := Config{
		Num:    1,
		Shards: [10]int{1, 1, 0, 0, 2, 2, 2, 0, 2, 0},
		Groups: make(map[int][]string),
	}

	shardCtler.AdjustConfig(&config)

	for _, v := range config.Shards {
		if v != 0 {
			panic("error")
		}
	}
}

func RandomGen(t *testing.T) {
	shardCtrler := ShardCtrler{}
	config := Config{
		Num:    1,
		Shards: [10]int{1, 1, 1, 2, 2, 2, 3, 3, 3, 3},
		Groups: make(map[int][]string),
	}

	gidCount := rand.Int()%20 + 1
	gids := make([]int, 0)
	for i := 1; i <= gidCount; i++ {
		gids = append(gids, i)
		config.Groups[i] = []string{}
	}
	for i := 0; i < NShards; i++ {
		config.Shards[i] = gids[rand.Int()%len(gids)]
		if rand.Int()%10 < 2 {
			config.Shards[i] = 0
		}
	}

	// 调整前
	before := config.Copy()
	avgShardsCount := NShards / len(config.Groups)
	otherShardsCount := NShards - avgShardsCount*len(config.Groups)

	shardCtrler.AdjustConfig(&config)
	sum := 0
	for _, gid := range gids {
		count := 0
		for _, v := range config.Shards {
			if v == gid {
				count++
			}
		}
		if count < avgShardsCount || count > avgShardsCount+otherShardsCount {
			fmt.Printf("gidCount:%d \n", gidCount)
			fmt.Printf("before:%+v \n", before.Shards)
			fmt.Printf("after:%+v \n", config.Shards)
			panic("error")
		}
		sum += count
	}
	if sum != NShards {
		fmt.Printf("before:%+v \n", before.Shards)
		fmt.Printf("after:%+v \n", config.Shards)
		panic("error")
	}
}

func TestShardCtrler_AdjustConfig2(t *testing.T) {
	for i := 0; i < 100000; i++ {
		RandomGen(t)
	}
}

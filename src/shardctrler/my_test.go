package shardctrler

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func loadblance(g map[int][]string) {
	arr := []int{}
	for k := range g {
		arr = append(arr, k)
	}

	sort.Ints(arr)
	shards := [NShards]int{}
	d := NShards % len(arr)
	cnt := NShards / len(arr)
	index := 0
	for _,v := range arr {
		for i := 0; i < cnt; i++ {
			shards[index] = v
			index++
		}
		if d > 0 {
			shards[index] = v
			index++
			d--
		}
	}
	fmt.Println(shards)
}

func TestXxx(t *testing.T) {
	g := make(map[int][]string)
	rand.Seed(time.Now().Unix())

	for jjj := 0; jjj < 10; jjj++ {
		for i := 1; i < 2+rand.Intn(10); i++ {
			g[i] = []string{"a", "b", "c"}
		}
		fmt.Printf("g len :%v\n", len(g))
		loadblance(g)
	}
}

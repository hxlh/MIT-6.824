package shardkv

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMap(t *testing.T) {
	shards := make(map[int][]int, 0)
	shards[0] = append(shards[0], 1000)
	shards[0] = append(shards[0], 2000)
}

func TestFindErr(t *testing.T) {
	for i := 1; i <= 2000; i++ {
		file, _ := os.Open(fmt.Sprintf("test-%v.log", i))
		buf, _ := io.ReadAll(file)
		txt := string(buf)
		if strings.Contains(txt, "FAIL") {
			fmt.Printf("test-%v.log\n", i)
		}
	}
}

func TestCheck(t *testing.T) {
	shards1 := []int{100, 100, 100, 101, 101, 102, 102, 102, 102, 102}
	shards2 := []int{101, 101, 101, 101, 101, 102, 102, 102, 102, 102}
	gidToshards := make(map[int][]int, 0)
	for i := 0; i < len(shards2); i++ {
		if shards2[i] != shards1[i] && shards2[i] == 100 {
			gidToshards[shards1[i]] = append(gidToshards[shards1[i]], i)
		}
	}
	fmt.Println(gidToshards)
}

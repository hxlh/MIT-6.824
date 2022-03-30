package raft

import (
	"fmt"
	"log"
	"testing"
)

func TestGenElectionTimeout(t *testing.T) {
	for i := 0; i < 100; i++ {

		log.Printf("%v ms\n", GenElectionTimeout())
	}
}

func BenchmarkGenElectionTimeou(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenElectionTimeout()
	}
}

func TestXxx(t *testing.T) {
	key := []int{
		10, 5, 46, 8,
	}
	key = key[:1]
	key = append(key, 100)
	fmt.Printf("key %v cap %v\n", key,cap(key))
}

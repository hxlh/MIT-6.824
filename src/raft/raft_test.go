package raft

import (
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

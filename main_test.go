package main

import (
	"testing"
)

func BenchmarkConsumeRecords(b *testing.B) {
        for n := 0; n < b.N; n++ {
                ConsumeRecords()
        }
}

func BenchmarkProduceRecords(b *testing.B) {
        for n := 0; n < b.N; n++ {
                ProduceRecords()
        }
}
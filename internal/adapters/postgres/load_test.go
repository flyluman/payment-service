package postgres

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

func BenchmarkShardIndexConcurrent(b *testing.B) {
	ids := make([]uuid.UUID, 1000)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ShardIndex(ids[i%len(ids)], 64)
			i++
		}
	})
}

func BenchmarkShardIndexDistributionLoad(b *testing.B) {
	const shardCount = 64
	const numIDs = 100000

	ids := make([]uuid.UUID, numIDs)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}

	var mu sync.Mutex
	dist := make([]int, shardCount)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := make([]int, shardCount)
		i := 0
		for pb.Next() {
			local[ShardIndex(ids[i%numIDs], shardCount)]++
			i++
		}
		mu.Lock()
		for s := range local {
			dist[s] += local[s]
		}
		mu.Unlock()
	})
	b.StopTimer()

	var min, max = dist[0], dist[0]
	for _, c := range dist {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	b.ReportMetric(float64(max)/float64(min+1), "skew")
}

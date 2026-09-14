package postgres

import (
	"testing"

	"github.com/google/uuid"
)

func BenchmarkShardIndex(b *testing.B) {
	ids := make([]uuid.UUID, 1000)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}
	shardCounts := []int{16, 32, 64, 128, 256}

	b.ResetTimer()
	for _, n := range shardCounts {
		b.Run("shards="+itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ShardIndex(ids[i%len(ids)], n)
			}
		})
	}
}

func BenchmarkShardIndexDistribution(b *testing.B) {
	const n = 64
	ids := make([]uuid.UUID, 10000)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}
	dist := make([]int, n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, id := range ids {
			dist[ShardIndex(id, n)]++
		}
	}
	b.StopTimer()

	var min, max int = dist[0], dist[0]
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

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [10]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

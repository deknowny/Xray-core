package bytespool

import "sync/atomic"

type PoolMetric struct {
	BucketBytes   int32
	InUse         int64
	InUseBytes    int64
	MaxInUseBytes int64
	AllocTotal    int64
	FreeTotal     int64
}

type MetricsSnapshot struct {
	Pools               []PoolMetric
	LargeInUse          int64
	LargeInUseBytes     int64
	LargeAllocTotal     int64
	LargeFreeTotal      int64
	MaxPooledBytesInUse int64
	MaxLargeBytesInUse  int64
}

var (
	metricPoolInUse      [numPools]atomic.Int64
	metricPoolInUseBytes [numPools]atomic.Int64
	metricPoolAllocTotal [numPools]atomic.Int64
	metricPoolFreeTotal  [numPools]atomic.Int64

	metricLargeInUse          atomic.Int64
	metricLargeInUseBytes     atomic.Int64
	metricLargeAllocTotal     atomic.Int64
	metricLargeFreeTotal      atomic.Int64
	metricMaxPooledBytesInUse atomic.Int64
	metricMaxLargeBytesInUse  atomic.Int64
)

func recordAlloc(idx int, capacity int32) {
	if idx < 0 {
		metricLargeInUse.Add(1)
		current := metricLargeInUseBytes.Add(int64(capacity))
		updateMax(&metricMaxLargeBytesInUse, current)
		metricLargeAllocTotal.Add(1)
		return
	}

	metricPoolInUse[idx].Add(1)
	current := metricPoolInUseBytes[idx].Add(int64(capacity))
	updateMax(&metricMaxPooledBytesInUse, currentPooledBytesInUse())
	updateMax(&metricPoolMaxBytes[idx], current)
	metricPoolAllocTotal[idx].Add(1)
}

func recordFree(idx int, capacity int32) {
	if idx < 0 {
		metricLargeInUse.Add(-1)
		metricLargeInUseBytes.Add(-int64(capacity))
		metricLargeFreeTotal.Add(1)
		return
	}

	metricPoolInUse[idx].Add(-1)
	metricPoolInUseBytes[idx].Add(-int64(capacity))
	metricPoolFreeTotal[idx].Add(1)
}

var metricPoolMaxBytes [numPools]atomic.Int64

func Metrics() MetricsSnapshot {
	pools := make([]PoolMetric, 0, numPools)
	for idx, size := range poolSize {
		pools = append(pools, PoolMetric{
			BucketBytes:   size,
			InUse:         metricPoolInUse[idx].Load(),
			InUseBytes:    metricPoolInUseBytes[idx].Load(),
			MaxInUseBytes: metricPoolMaxBytes[idx].Load(),
			AllocTotal:    metricPoolAllocTotal[idx].Load(),
			FreeTotal:     metricPoolFreeTotal[idx].Load(),
		})
	}
	return MetricsSnapshot{
		Pools:               pools,
		LargeInUse:          metricLargeInUse.Load(),
		LargeInUseBytes:     metricLargeInUseBytes.Load(),
		LargeAllocTotal:     metricLargeAllocTotal.Load(),
		LargeFreeTotal:      metricLargeFreeTotal.Load(),
		MaxPooledBytesInUse: metricMaxPooledBytesInUse.Load(),
		MaxLargeBytesInUse:  metricMaxLargeBytesInUse.Load(),
	}
}

func currentPooledBytesInUse() int64 {
	var total int64
	for idx := range poolSize {
		total += metricPoolInUseBytes[idx].Load()
	}
	return total
}

func updateMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current {
			return
		}
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

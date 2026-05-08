package buf

import "sync/atomic"

type MetricsSnapshot struct {
	ManagedInUse                 int64
	ManagedBytesInUse            int64
	ManagedMaxBytesInUse         int64
	ManagedGetsTotal             int64
	ManagedReleasesTotal         int64
	ManagedFallbackAllocsTotal   int64
	ManagedDroppedTotal          int64
	ManagedDroppedBytesTotal     int64
	BytespoolOwnedInUse          int64
	BytespoolOwnedBytesInUse     int64
	BytespoolOwnedMaxBytesInUse  int64
	BytespoolOwnedGetsTotal      int64
	BytespoolOwnedReleasesTotal  int64
	BytespoolOwnedRequestedTotal int64
}

var (
	metricManagedInUse               atomic.Int64
	metricManagedBytesInUse          atomic.Int64
	metricManagedMaxBytesInUse       atomic.Int64
	metricManagedGetsTotal           atomic.Int64
	metricManagedReleasesTotal       atomic.Int64
	metricManagedFallbackAllocsTotal atomic.Int64
	metricManagedDroppedTotal        atomic.Int64
	metricManagedDroppedBytesTotal   atomic.Int64

	metricBytespoolOwnedInUse          atomic.Int64
	metricBytespoolOwnedBytesInUse     atomic.Int64
	metricBytespoolOwnedMaxBytesInUse  atomic.Int64
	metricBytespoolOwnedGetsTotal      atomic.Int64
	metricBytespoolOwnedReleasesTotal  atomic.Int64
	metricBytespoolOwnedRequestedTotal atomic.Int64
)

func recordManagedBufferGet(capacity int, fallbackAlloc bool) {
	metricManagedInUse.Add(1)
	current := metricManagedBytesInUse.Add(int64(capacity))
	updateMetricMax(&metricManagedMaxBytesInUse, current)
	metricManagedGetsTotal.Add(1)
	if fallbackAlloc {
		metricManagedFallbackAllocsTotal.Add(1)
	}
}

func recordManagedBufferRelease(capacity int) {
	metricManagedInUse.Add(-1)
	metricManagedBytesInUse.Add(-int64(capacity))
	metricManagedReleasesTotal.Add(1)
}

func recordManagedBufferDropped(capacity int) {
	metricManagedDroppedTotal.Add(1)
	metricManagedDroppedBytesTotal.Add(int64(capacity))
}

func recordBytespoolOwnedBufferGet(requested int32, capacity int) {
	metricBytespoolOwnedInUse.Add(1)
	current := metricBytespoolOwnedBytesInUse.Add(int64(capacity))
	updateMetricMax(&metricBytespoolOwnedMaxBytesInUse, current)
	metricBytespoolOwnedGetsTotal.Add(1)
	metricBytespoolOwnedRequestedTotal.Add(int64(requested))
}

func recordBytespoolOwnedBufferRelease(capacity int) {
	metricBytespoolOwnedInUse.Add(-1)
	metricBytespoolOwnedBytesInUse.Add(-int64(capacity))
	metricBytespoolOwnedReleasesTotal.Add(1)
}

func Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		ManagedInUse:                 metricManagedInUse.Load(),
		ManagedBytesInUse:            metricManagedBytesInUse.Load(),
		ManagedMaxBytesInUse:         metricManagedMaxBytesInUse.Load(),
		ManagedGetsTotal:             metricManagedGetsTotal.Load(),
		ManagedReleasesTotal:         metricManagedReleasesTotal.Load(),
		ManagedFallbackAllocsTotal:   metricManagedFallbackAllocsTotal.Load(),
		ManagedDroppedTotal:          metricManagedDroppedTotal.Load(),
		ManagedDroppedBytesTotal:     metricManagedDroppedBytesTotal.Load(),
		BytespoolOwnedInUse:          metricBytespoolOwnedInUse.Load(),
		BytespoolOwnedBytesInUse:     metricBytespoolOwnedBytesInUse.Load(),
		BytespoolOwnedMaxBytesInUse:  metricBytespoolOwnedMaxBytesInUse.Load(),
		BytespoolOwnedGetsTotal:      metricBytespoolOwnedGetsTotal.Load(),
		BytespoolOwnedReleasesTotal:  metricBytespoolOwnedReleasesTotal.Load(),
		BytespoolOwnedRequestedTotal: metricBytespoolOwnedRequestedTotal.Load(),
	}
}

func updateMetricMax(target *atomic.Int64, value int64) {
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

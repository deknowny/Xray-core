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
	CopyActive                   int64
	CopyStartedTotal             int64
	CopyCompletedTotal           int64
	CopyReadErrorTotal           int64
	CopyWriteErrorTotal          int64
	CopyReadBatchesTotal         int64
	CopyWriteBatchesTotal        int64
	CopyReadBytesTotal           int64
	CopyWriteBytesTotal          int64
	CopyMaxBatchBytes            int64
	CopyBatchBytesBuckets        []BucketMetric
}

type BucketMetric struct {
	Le    string
	Value int64
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

	metricCopyActive            atomic.Int64
	metricCopyStartedTotal      atomic.Int64
	metricCopyCompletedTotal    atomic.Int64
	metricCopyReadErrorTotal    atomic.Int64
	metricCopyWriteErrorTotal   atomic.Int64
	metricCopyReadBatchesTotal  atomic.Int64
	metricCopyWriteBatchesTotal atomic.Int64
	metricCopyReadBytesTotal    atomic.Int64
	metricCopyWriteBytesTotal   atomic.Int64
	metricCopyMaxBatchBytes     atomic.Int64
)

var copyBatchBytesBuckets = []int64{
	0,
	1024,
	4 * 1024,
	8 * 1024,
	16 * 1024,
	32 * 1024,
	64 * 1024,
	128 * 1024,
	256 * 1024,
	512 * 1024,
}

var metricCopyBatchBytesBuckets [10]atomic.Int64

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

func recordCopyStarted() {
	metricCopyActive.Add(1)
	metricCopyStartedTotal.Add(1)
}

func recordCopyCompleted(err error) {
	metricCopyActive.Add(-1)
	metricCopyCompletedTotal.Add(1)
	if err == nil {
		return
	}
	if IsWriteError(err) {
		metricCopyWriteErrorTotal.Add(1)
	} else {
		metricCopyReadErrorTotal.Add(1)
	}
}

func recordCopyReadBatch(bytes int64) {
	metricCopyReadBatchesTotal.Add(1)
	metricCopyReadBytesTotal.Add(bytes)
	updateMetricMax(&metricCopyMaxBatchBytes, bytes)
	for idx, bucket := range copyBatchBytesBuckets {
		if bytes <= bucket {
			metricCopyBatchBytesBuckets[idx].Add(1)
		}
	}
}

func recordCopyWriteBatch(bytes int64) {
	metricCopyWriteBatchesTotal.Add(1)
	metricCopyWriteBytesTotal.Add(bytes)
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
		CopyActive:                   metricCopyActive.Load(),
		CopyStartedTotal:             metricCopyStartedTotal.Load(),
		CopyCompletedTotal:           metricCopyCompletedTotal.Load(),
		CopyReadErrorTotal:           metricCopyReadErrorTotal.Load(),
		CopyWriteErrorTotal:          metricCopyWriteErrorTotal.Load(),
		CopyReadBatchesTotal:         metricCopyReadBatchesTotal.Load(),
		CopyWriteBatchesTotal:        metricCopyWriteBatchesTotal.Load(),
		CopyReadBytesTotal:           metricCopyReadBytesTotal.Load(),
		CopyWriteBytesTotal:          metricCopyWriteBytesTotal.Load(),
		CopyMaxBatchBytes:            metricCopyMaxBatchBytes.Load(),
		CopyBatchBytesBuckets:        snapshotCopyBatchBytesBuckets(),
	}
}

func snapshotCopyBatchBytesBuckets() []BucketMetric {
	values := make([]BucketMetric, 0, len(copyBatchBytesBuckets)+1)
	for idx, bucket := range copyBatchBytesBuckets {
		values = append(values, BucketMetric{
			Le:    formatInt(bucket),
			Value: metricCopyBatchBytesBuckets[idx].Load(),
		})
	}
	values = append(values, BucketMetric{
		Le:    "+Inf",
		Value: metricCopyReadBatchesTotal.Load(),
	})
	return values
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

func formatInt(value int64) string {
	if value == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}

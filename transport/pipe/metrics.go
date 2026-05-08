package pipe

import (
	"errors"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

type MetricValue struct {
	Labels []string
	Value  int64
}

type MetricsSnapshot struct {
	Active               int64
	CreatedTotal         int64
	BufferedBytes        int64
	BufferedBuffers      int64
	MaxBufferedBytes     int64
	MaxBufferedBuffers   int64
	MaxPipeBufferedBytes int64
	WritesTotal          int64
	WriteBytesTotal      int64
	ReadsTotal           int64
	ReadBytesTotal       int64
	BufferFullTotal      int64
	DiscardOverflowTotal int64
	WriteClosedTotal     int64
	WriteErrorTotal      int64
	CreatedByLimit       []MetricValue
	ActiveByLimit        []MetricValue
	QueueBytesBuckets    []MetricValue
}

var (
	metricActive               atomic.Int64
	metricCreatedTotal         atomic.Int64
	metricBufferedBytes        atomic.Int64
	metricBufferedBuffers      atomic.Int64
	metricMaxBufferedBytes     atomic.Int64
	metricMaxBufferedBuffers   atomic.Int64
	metricMaxPipeBufferedBytes atomic.Int64
	metricWritesTotal          atomic.Int64
	metricWriteBytesTotal      atomic.Int64
	metricReadsTotal           atomic.Int64
	metricReadBytesTotal       atomic.Int64
	metricBufferFullTotal      atomic.Int64
	metricDiscardOverflowTotal atomic.Int64
	metricWriteClosedTotal     atomic.Int64
	metricWriteErrorTotal      atomic.Int64

	metricCreatedByLimit sync.Map
	metricActiveByLimit  sync.Map
)

var queueBytesBuckets = []int64{
	0,
	8 * 1024,
	16 * 1024,
	32 * 1024,
	64 * 1024,
	128 * 1024,
	256 * 1024,
	512 * 1024,
	1024 * 1024,
	2 * 1024 * 1024,
}

var metricQueueBytesBuckets [10]atomic.Int64

func recordPipeCreated(limit int32) {
	metricActive.Add(1)
	metricCreatedTotal.Add(1)
	addLimitMetric(&metricCreatedByLimit, limit, 1)
	addLimitMetric(&metricActiveByLimit, limit, 1)
}

func recordPipeClosed(limit int32) {
	metricActive.Add(-1)
	addLimitMetric(&metricActiveByLimit, limit, -1)
}

func recordPipeQueuedDelta(bytes int64, buffers int64) {
	if bytes != 0 {
		current := metricBufferedBytes.Add(bytes)
		updateMax(&metricMaxBufferedBytes, current)
	}
	if buffers != 0 {
		current := metricBufferedBuffers.Add(buffers)
		updateMax(&metricMaxBufferedBuffers, current)
	}
}

func recordPipeWrite(bytes int64, pipeBytes int64) {
	metricWritesTotal.Add(1)
	metricWriteBytesTotal.Add(bytes)
	updateMax(&metricMaxPipeBufferedBytes, pipeBytes)
	observeQueueBytes(pipeBytes)
}

func recordPipeRead(bytes int64) {
	metricReadsTotal.Add(1)
	metricReadBytesTotal.Add(bytes)
}

func recordPipeWriteError(err error) {
	switch {
	case errors.Is(err, errBufferFull):
		metricBufferFullTotal.Add(1)
	case errors.Is(err, io.ErrClosedPipe):
		metricWriteClosedTotal.Add(1)
	default:
		metricWriteErrorTotal.Add(1)
	}
}

func recordPipeDiscardOverflow() {
	metricDiscardOverflowTotal.Add(1)
}

func Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Active:               metricActive.Load(),
		CreatedTotal:         metricCreatedTotal.Load(),
		BufferedBytes:        metricBufferedBytes.Load(),
		BufferedBuffers:      metricBufferedBuffers.Load(),
		MaxBufferedBytes:     metricMaxBufferedBytes.Load(),
		MaxBufferedBuffers:   metricMaxBufferedBuffers.Load(),
		MaxPipeBufferedBytes: metricMaxPipeBufferedBytes.Load(),
		WritesTotal:          metricWritesTotal.Load(),
		WriteBytesTotal:      metricWriteBytesTotal.Load(),
		ReadsTotal:           metricReadsTotal.Load(),
		ReadBytesTotal:       metricReadBytesTotal.Load(),
		BufferFullTotal:      metricBufferFullTotal.Load(),
		DiscardOverflowTotal: metricDiscardOverflowTotal.Load(),
		WriteClosedTotal:     metricWriteClosedTotal.Load(),
		WriteErrorTotal:      metricWriteErrorTotal.Load(),
		CreatedByLimit:       snapshotMap(&metricCreatedByLimit),
		ActiveByLimit:        snapshotMap(&metricActiveByLimit),
		QueueBytesBuckets:    snapshotQueueBytesBuckets(),
	}
}

func addLimitMetric(store *sync.Map, limit int32, delta int64) {
	key := limitLabel(limit)
	value, _ := store.LoadOrStore(key, &atomic.Int64{})
	value.(*atomic.Int64).Add(delta)
}

func limitLabel(limit int32) string {
	if limit < 0 {
		return "unlimited"
	}
	return stringInt(int64(limit))
}

func observeQueueBytes(bytes int64) {
	for idx, bucket := range queueBytesBuckets {
		if bytes <= bucket {
			metricQueueBytesBuckets[idx].Add(1)
		}
	}
}

func snapshotQueueBytesBuckets() []MetricValue {
	values := make([]MetricValue, 0, len(queueBytesBuckets)+1)
	for idx, bucket := range queueBytesBuckets {
		values = append(values, MetricValue{
			Labels: []string{stringInt(bucket)},
			Value:  metricQueueBytesBuckets[idx].Load(),
		})
	}
	values = append(values, MetricValue{
		Labels: []string{"+Inf"},
		Value:  metricWritesTotal.Load(),
	})
	return values
}

func snapshotMap(store *sync.Map) []MetricValue {
	values := make([]MetricValue, 0)
	store.Range(func(key interface{}, value interface{}) bool {
		values = append(values, MetricValue{
			Labels: []string{key.(string)},
			Value:  value.(*atomic.Int64).Load(),
		})
		return true
	})
	sort.Slice(values, func(i, j int) bool {
		return values[i].Labels[0] < values[j].Labels[0]
	})
	return values
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

func stringInt(value int64) string {
	if value == 0 {
		return "0"
	}

	negative := value < 0
	if negative {
		value = -value
	}

	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

package metrics

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/bytespool"
	xerrors "github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/extension"
	feature_stats "github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport/pipe"
)

const prometheusBuildBaseTag = "v26.3.27"

var activePrometheusHandler atomic.Value
var prometheusStartedAt = time.Now()

var (
	inboundActive          sync.Map
	inboundTotal           sync.Map
	inboundClosed          sync.Map
	inboundDurationBuckets sync.Map
	inboundDurationSum     sync.Map
	inboundDurationCount   sync.Map

	outboundActive          sync.Map
	outboundTotal           sync.Map
	outboundClosed          sync.Map
	outboundDurationBuckets sync.Map
	outboundDurationSum     sync.Map
	outboundDurationCount   sync.Map

	outboundDialTotal           sync.Map
	outboundDialDurationBuckets sync.Map
	outboundDialDurationSum     sync.Map
	outboundDialDurationCount   sync.Map

	balancerSelected sync.Map
	balancerErrors   sync.Map
)

var durationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

func init() {
	http.HandleFunc("/metrics", prometheusHandler)
}

func setActivePrometheusHandler(handler *MetricsHandler) {
	activePrometheusHandler.Store(handler)
}

func StartInboundConnection(tag string, network string) func(error) {
	tag = normalizeLabel(tag, "untagged")
	network = normalizeLabel(network, "unknown")
	key := labelsKey(tag, network)

	addMetric(&inboundActive, key, 1)
	addMetric(&inboundTotal, key, 1)
	startedAt := time.Now()

	return func(err error) {
		addMetric(&inboundActive, key, -1)
		result := ClassifyError(err)
		addMetric(&inboundClosed, labelsKey(tag, network, result), 1)
		observeDuration(&inboundDurationBuckets, &inboundDurationSum, &inboundDurationCount, []string{tag, network, result}, time.Since(startedAt))
	}
}

func StartOutboundDispatch(tag string, network string) func(error) {
	tag = normalizeLabel(tag, "untagged")
	network = normalizeLabel(network, "unknown")
	key := labelsKey(tag, network)

	addMetric(&outboundActive, key, 1)
	addMetric(&outboundTotal, key, 1)
	startedAt := time.Now()

	return func(err error) {
		addMetric(&outboundActive, key, -1)
		result := ClassifyError(err)
		addMetric(&outboundClosed, labelsKey(tag, network, result), 1)
		observeDuration(&outboundDurationBuckets, &outboundDurationSum, &outboundDurationCount, []string{tag, network, result}, time.Since(startedAt))
	}
}

func ObserveOutboundDial(tag string, network string, err error, duration time.Duration) {
	tag = normalizeLabel(tag, "untagged")
	network = normalizeLabel(network, "unknown")
	result := ClassifyError(err)

	addMetric(&outboundDialTotal, labelsKey(tag, network, result), 1)
	observeDuration(&outboundDialDurationBuckets, &outboundDialDurationSum, &outboundDialDurationCount, []string{tag, network, result}, duration)
}

func RecordBalancerSelected(balancer string, selected string) {
	addMetric(&balancerSelected, labelsKey(normalizeLabel(balancer, "untagged"), normalizeLabel(selected, "empty")), 1)
}

func RecordBalancerError(balancer string, reason string) {
	addMetric(&balancerErrors, labelsKey(normalizeLabel(balancer, "untagged"), normalizeLabel(reason, "other")), 1)
}

func ClassifyError(err error) string {
	if err == nil {
		return "ok"
	}

	cause := xerrors.Cause(err)
	if cause == nil {
		cause = err
	}

	switch {
	case stderrors.Is(cause, context.Canceled):
		return "canceled"
	case stderrors.Is(cause, io.EOF):
		return "eof"
	case stderrors.Is(cause, io.ErrClosedPipe):
		return "closed_pipe"
	}

	message := strings.ToLower(cause.Error())
	switch {
	case strings.Contains(message, "timeout") || strings.Contains(message, "timed out") || strings.Contains(message, "deadline"):
		return "timeout"
	case strings.Contains(message, "connection refused"):
		return "refused"
	case strings.Contains(message, "connection reset") || strings.Contains(message, "reset by peer"):
		return "reset"
	case strings.Contains(message, "broken pipe"):
		return "broken_pipe"
	case strings.Contains(message, "no route") || strings.Contains(message, "network is unreachable"):
		return "network_unreachable"
	case strings.Contains(message, "lookup") || strings.Contains(message, "dns"):
		return "dns"
	case strings.Contains(message, "authentication") || strings.Contains(message, "unauthorized") || strings.Contains(message, "invalid user"):
		return "auth"
	default:
		return "other"
	}
}

func prometheusHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	writeBuildInfo(w)
	writeRuntimeMetrics(w)
	writeProcessMemoryMetrics(w)
	writeBufferMetrics(w)
	writeBytespoolMetrics(w)
	writePipeMetrics(w)

	if value := activePrometheusHandler.Load(); value != nil {
		if handler, ok := value.(*MetricsHandler); ok {
			writeStatsMetrics(w, handler.statsManager)
			writeObservatoryMetrics(w, handler.observatory)
		}
	}

	writeGaugeVec(w, "xray_inbound_connections_active", "Active inbound connections.", []string{"tag", "network"}, &inboundActive)
	writeCounterVec(w, "xray_inbound_connections_total", "Accepted inbound connections.", []string{"tag", "network"}, &inboundTotal)
	writeCounterVec(w, "xray_inbound_connections_closed_total", "Closed inbound connections by result.", []string{"tag", "network", "result"}, &inboundClosed)
	writeHistogram(w, "xray_inbound_connection_duration_seconds", "Inbound connection lifetime.", []string{"tag", "network", "result"}, &inboundDurationBuckets, &inboundDurationSum, &inboundDurationCount)

	writeGaugeVec(w, "xray_outbound_dispatch_active", "Active outbound dispatches.", []string{"tag", "network"}, &outboundActive)
	writeCounterVec(w, "xray_outbound_dispatch_total", "Started outbound dispatches.", []string{"tag", "network"}, &outboundTotal)
	writeCounterVec(w, "xray_outbound_dispatch_closed_total", "Closed outbound dispatches by result.", []string{"tag", "network", "result"}, &outboundClosed)
	writeHistogram(w, "xray_outbound_dispatch_duration_seconds", "Outbound dispatch lifetime.", []string{"tag", "network", "result"}, &outboundDurationBuckets, &outboundDurationSum, &outboundDurationCount)

	writeCounterVec(w, "xray_outbound_dial_total", "Outbound network dial attempts by result.", []string{"tag", "network", "result"}, &outboundDialTotal)
	writeHistogram(w, "xray_outbound_dial_duration_seconds", "Outbound network dial duration.", []string{"tag", "network", "result"}, &outboundDialDurationBuckets, &outboundDialDurationSum, &outboundDialDurationCount)

	writeCounterVec(w, "xray_balancer_selected_total", "Balancer selections by selected outbound.", []string{"balancer", "selected"}, &balancerSelected)
	writeCounterVec(w, "xray_balancer_errors_total", "Balancer selection errors by reason.", []string{"balancer", "reason"}, &balancerErrors)
}

func writeBuildInfo(w io.Writer) {
	fmt.Fprintln(w, "# HELP xray_mtconf_build_info Mtconf Xray build metadata.")
	fmt.Fprintln(w, "# TYPE xray_mtconf_build_info gauge")
	fmt.Fprintf(w, "xray_mtconf_build_info{base_tag=%s,metrics=%s} 1\n", quoteLabel(prometheusBuildBaseTag), quoteLabel("mtconf"))
}

func writeRuntimeMetrics(w io.Writer) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Fprintln(w, "# HELP xray_uptime_seconds Xray process uptime.")
	fmt.Fprintln(w, "# TYPE xray_uptime_seconds gauge")
	fmt.Fprintf(w, "xray_uptime_seconds %.3f\n", time.Since(prometheusStartedAt).Seconds())

	fmt.Fprintln(w, "# HELP xray_runtime_goroutines Go goroutines.")
	fmt.Fprintln(w, "# TYPE xray_runtime_goroutines gauge")
	fmt.Fprintf(w, "xray_runtime_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintln(w, "# HELP xray_runtime_heap_alloc_bytes Go heap bytes allocated.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_alloc_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_heap_alloc_bytes %d\n", mem.HeapAlloc)

	fmt.Fprintln(w, "# HELP xray_runtime_heap_sys_bytes Go heap bytes obtained from the OS.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_sys_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_heap_sys_bytes %d\n", mem.HeapSys)

	fmt.Fprintln(w, "# HELP xray_runtime_heap_idle_bytes Go heap bytes in idle spans.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_idle_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_heap_idle_bytes %d\n", mem.HeapIdle)

	fmt.Fprintln(w, "# HELP xray_runtime_heap_inuse_bytes Go heap bytes in active spans.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_inuse_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_heap_inuse_bytes %d\n", mem.HeapInuse)

	fmt.Fprintln(w, "# HELP xray_runtime_heap_released_bytes Go heap bytes released to the OS.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_released_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_heap_released_bytes %d\n", mem.HeapReleased)

	fmt.Fprintln(w, "# HELP xray_runtime_heap_objects Go heap object count.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_objects gauge")
	fmt.Fprintf(w, "xray_runtime_heap_objects %d\n", mem.HeapObjects)

	fmt.Fprintln(w, "# HELP xray_runtime_alloc_bytes_total Total bytes allocated by Go.")
	fmt.Fprintln(w, "# TYPE xray_runtime_alloc_bytes_total counter")
	fmt.Fprintf(w, "xray_runtime_alloc_bytes_total %d\n", mem.TotalAlloc)

	fmt.Fprintln(w, "# HELP xray_runtime_mallocs_total Total Go heap allocation count.")
	fmt.Fprintln(w, "# TYPE xray_runtime_mallocs_total counter")
	fmt.Fprintf(w, "xray_runtime_mallocs_total %d\n", mem.Mallocs)

	fmt.Fprintln(w, "# HELP xray_runtime_frees_total Total Go heap free count.")
	fmt.Fprintln(w, "# TYPE xray_runtime_frees_total counter")
	fmt.Fprintf(w, "xray_runtime_frees_total %d\n", mem.Frees)

	fmt.Fprintln(w, "# HELP xray_runtime_sys_bytes Go bytes obtained from the OS.")
	fmt.Fprintln(w, "# TYPE xray_runtime_sys_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_sys_bytes %d\n", mem.Sys)

	fmt.Fprintln(w, "# HELP xray_runtime_stack_inuse_bytes Go stack bytes in use.")
	fmt.Fprintln(w, "# TYPE xray_runtime_stack_inuse_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_stack_inuse_bytes %d\n", mem.StackInuse)

	fmt.Fprintln(w, "# HELP xray_runtime_stack_sys_bytes Go stack bytes obtained from the OS.")
	fmt.Fprintln(w, "# TYPE xray_runtime_stack_sys_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_stack_sys_bytes %d\n", mem.StackSys)

	fmt.Fprintln(w, "# HELP xray_runtime_mspan_inuse_bytes Go mspan bytes in use.")
	fmt.Fprintln(w, "# TYPE xray_runtime_mspan_inuse_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_mspan_inuse_bytes %d\n", mem.MSpanInuse)

	fmt.Fprintln(w, "# HELP xray_runtime_mcache_inuse_bytes Go mcache bytes in use.")
	fmt.Fprintln(w, "# TYPE xray_runtime_mcache_inuse_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_mcache_inuse_bytes %d\n", mem.MCacheInuse)

	fmt.Fprintln(w, "# HELP xray_runtime_next_gc_bytes Go heap target for the next GC.")
	fmt.Fprintln(w, "# TYPE xray_runtime_next_gc_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_next_gc_bytes %d\n", mem.NextGC)

	fmt.Fprintln(w, "# HELP xray_runtime_memory_limit_bytes Go runtime memory limit.")
	fmt.Fprintln(w, "# TYPE xray_runtime_memory_limit_bytes gauge")
	fmt.Fprintf(w, "xray_runtime_memory_limit_bytes %d\n", debug.SetMemoryLimit(-1))

	fmt.Fprintln(w, "# HELP xray_runtime_gc_cpu_fraction Recent fraction of CPU used by Go GC.")
	fmt.Fprintln(w, "# TYPE xray_runtime_gc_cpu_fraction gauge")
	fmt.Fprintf(w, "xray_runtime_gc_cpu_fraction %.9f\n", mem.GCCPUFraction)

	fmt.Fprintln(w, "# HELP xray_runtime_gc_total Go GC cycles.")
	fmt.Fprintln(w, "# TYPE xray_runtime_gc_total counter")
	fmt.Fprintf(w, "xray_runtime_gc_total %d\n", mem.NumGC)

	fmt.Fprintln(w, "# HELP xray_runtime_gc_pause_seconds_total Total Go GC pause time.")
	fmt.Fprintln(w, "# TYPE xray_runtime_gc_pause_seconds_total counter")
	fmt.Fprintf(w, "xray_runtime_gc_pause_seconds_total %.6f\n", float64(mem.PauseTotalNs)/float64(time.Second))

	fmt.Fprintln(w, "# HELP xray_runtime_gc_last_pause_seconds Last Go GC pause duration.")
	fmt.Fprintln(w, "# TYPE xray_runtime_gc_last_pause_seconds gauge")
	lastPause := uint64(0)
	if mem.NumGC > 0 {
		lastPause = mem.PauseNs[(mem.NumGC+255)%256]
	}
	fmt.Fprintf(w, "xray_runtime_gc_last_pause_seconds %.9f\n", float64(lastPause)/float64(time.Second))
}

func writeProcessMemoryMetrics(w io.Writer) {
	if statm, ok := readProcSelfStatm(); ok {
		fmt.Fprintln(w, "# HELP xray_process_virtual_memory_bytes Process virtual memory from /proc/self/statm.")
		fmt.Fprintln(w, "# TYPE xray_process_virtual_memory_bytes gauge")
		fmt.Fprintf(w, "xray_process_virtual_memory_bytes %d\n", statm.sizeBytes)

		fmt.Fprintln(w, "# HELP xray_process_resident_memory_bytes Process resident memory from /proc/self/statm.")
		fmt.Fprintln(w, "# TYPE xray_process_resident_memory_bytes gauge")
		fmt.Fprintf(w, "xray_process_resident_memory_bytes %d\n", statm.residentBytes)

		fmt.Fprintln(w, "# HELP xray_process_shared_memory_bytes Process shared resident memory from /proc/self/statm.")
		fmt.Fprintln(w, "# TYPE xray_process_shared_memory_bytes gauge")
		fmt.Fprintf(w, "xray_process_shared_memory_bytes %d\n", statm.sharedBytes)

		fmt.Fprintln(w, "# HELP xray_process_data_memory_bytes Process data and stack memory from /proc/self/statm.")
		fmt.Fprintln(w, "# TYPE xray_process_data_memory_bytes gauge")
		fmt.Fprintf(w, "xray_process_data_memory_bytes %d\n", statm.dataBytes)
	}

	if value, ok := readUintFile("/sys/fs/cgroup/memory.current"); ok {
		fmt.Fprintln(w, "# HELP xray_cgroup_memory_current_bytes Current cgroup memory usage.")
		fmt.Fprintln(w, "# TYPE xray_cgroup_memory_current_bytes gauge")
		fmt.Fprintf(w, "xray_cgroup_memory_current_bytes %d\n", value)
	}
	if value, ok := readUintFile("/sys/fs/cgroup/memory.max"); ok {
		fmt.Fprintln(w, "# HELP xray_cgroup_memory_limit_bytes Cgroup memory limit, -1 means unlimited.")
		fmt.Fprintln(w, "# TYPE xray_cgroup_memory_limit_bytes gauge")
		fmt.Fprintf(w, "xray_cgroup_memory_limit_bytes %d\n", value)
	}
}

func writeBufferMetrics(w io.Writer) {
	snapshot := buf.Metrics()

	fmt.Fprintln(w, "# HELP xray_buf_managed_buffers_in_use Managed 8KiB buf.Buffer objects currently checked out.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_buffers_in_use gauge")
	fmt.Fprintf(w, "xray_buf_managed_buffers_in_use %d\n", snapshot.ManagedInUse)

	fmt.Fprintln(w, "# HELP xray_buf_managed_bytes_in_use Managed buf.Buffer capacity currently checked out.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_bytes_in_use gauge")
	fmt.Fprintf(w, "xray_buf_managed_bytes_in_use %d\n", snapshot.ManagedBytesInUse)

	fmt.Fprintln(w, "# HELP xray_buf_managed_max_bytes_in_use High-water mark for managed buf.Buffer capacity.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_max_bytes_in_use gauge")
	fmt.Fprintf(w, "xray_buf_managed_max_bytes_in_use %d\n", snapshot.ManagedMaxBytesInUse)

	fmt.Fprintln(w, "# HELP xray_buf_managed_gets_total Managed buf.Buffer checkout count.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_gets_total counter")
	fmt.Fprintf(w, "xray_buf_managed_gets_total %d\n", snapshot.ManagedGetsTotal)

	fmt.Fprintln(w, "# HELP xray_buf_managed_releases_total Managed buf.Buffer release count.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_releases_total counter")
	fmt.Fprintf(w, "xray_buf_managed_releases_total %d\n", snapshot.ManagedReleasesTotal)

	fmt.Fprintln(w, "# HELP xray_buf_managed_fallback_allocs_total Managed buf.Buffer fallback allocation count.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_fallback_allocs_total counter")
	fmt.Fprintf(w, "xray_buf_managed_fallback_allocs_total %d\n", snapshot.ManagedFallbackAllocsTotal)

	fmt.Fprintln(w, "# HELP xray_buf_managed_dropped_total Managed buf.Buffer releases not returned to the 8KiB pool because capacity drifted.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_dropped_total counter")
	fmt.Fprintf(w, "xray_buf_managed_dropped_total %d\n", snapshot.ManagedDroppedTotal)

	fmt.Fprintln(w, "# HELP xray_buf_managed_dropped_bytes_total Managed buf.Buffer capacity dropped instead of returned to the 8KiB pool.")
	fmt.Fprintln(w, "# TYPE xray_buf_managed_dropped_bytes_total counter")
	fmt.Fprintf(w, "xray_buf_managed_dropped_bytes_total %d\n", snapshot.ManagedDroppedBytesTotal)

	fmt.Fprintln(w, "# HELP xray_buf_bytespool_owned_buffers_in_use buf.Buffer objects backed by bytespool currently checked out.")
	fmt.Fprintln(w, "# TYPE xray_buf_bytespool_owned_buffers_in_use gauge")
	fmt.Fprintf(w, "xray_buf_bytespool_owned_buffers_in_use %d\n", snapshot.BytespoolOwnedInUse)

	fmt.Fprintln(w, "# HELP xray_buf_bytespool_owned_bytes_in_use bytespool-backed buf.Buffer capacity currently checked out.")
	fmt.Fprintln(w, "# TYPE xray_buf_bytespool_owned_bytes_in_use gauge")
	fmt.Fprintf(w, "xray_buf_bytespool_owned_bytes_in_use %d\n", snapshot.BytespoolOwnedBytesInUse)

	fmt.Fprintln(w, "# HELP xray_buf_bytespool_owned_max_bytes_in_use High-water mark for bytespool-backed buf.Buffer capacity.")
	fmt.Fprintln(w, "# TYPE xray_buf_bytespool_owned_max_bytes_in_use gauge")
	fmt.Fprintf(w, "xray_buf_bytespool_owned_max_bytes_in_use %d\n", snapshot.BytespoolOwnedMaxBytesInUse)
}

func writeBytespoolMetrics(w io.Writer) {
	snapshot := bytespool.Metrics()

	fmt.Fprintln(w, "# HELP xray_bytespool_buffers_in_use bytespool slices currently checked out by bucket.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_buffers_in_use gauge")
	fmt.Fprintln(w, "# HELP xray_bytespool_bytes_in_use bytespool slice capacity currently checked out by bucket.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_bytes_in_use gauge")
	fmt.Fprintln(w, "# HELP xray_bytespool_max_bytes_in_use bytespool slice capacity high-water mark by bucket.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_max_bytes_in_use gauge")
	fmt.Fprintln(w, "# HELP xray_bytespool_allocs_total bytespool checkout count by bucket.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_allocs_total counter")
	fmt.Fprintln(w, "# HELP xray_bytespool_frees_total bytespool release count by bucket.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_frees_total counter")
	for _, pool := range snapshot.Pools {
		bucket := quoteLabel(strconv.FormatInt(int64(pool.BucketBytes), 10))
		fmt.Fprintf(w, "xray_bytespool_buffers_in_use{bucket_bytes=%s} %d\n", bucket, pool.InUse)
		fmt.Fprintf(w, "xray_bytespool_bytes_in_use{bucket_bytes=%s} %d\n", bucket, pool.InUseBytes)
		fmt.Fprintf(w, "xray_bytespool_max_bytes_in_use{bucket_bytes=%s} %d\n", bucket, pool.MaxInUseBytes)
		fmt.Fprintf(w, "xray_bytespool_allocs_total{bucket_bytes=%s} %d\n", bucket, pool.AllocTotal)
		fmt.Fprintf(w, "xray_bytespool_frees_total{bucket_bytes=%s} %d\n", bucket, pool.FreeTotal)
	}

	fmt.Fprintln(w, "# HELP xray_bytespool_large_buffers_in_use bytespool large slices currently checked out.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_large_buffers_in_use gauge")
	fmt.Fprintf(w, "xray_bytespool_large_buffers_in_use %d\n", snapshot.LargeInUse)

	fmt.Fprintln(w, "# HELP xray_bytespool_large_bytes_in_use bytespool large slice capacity currently checked out.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_large_bytes_in_use gauge")
	fmt.Fprintf(w, "xray_bytespool_large_bytes_in_use %d\n", snapshot.LargeInUseBytes)

	fmt.Fprintln(w, "# HELP xray_bytespool_large_allocs_total bytespool large slice checkout count.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_large_allocs_total counter")
	fmt.Fprintf(w, "xray_bytespool_large_allocs_total %d\n", snapshot.LargeAllocTotal)

	fmt.Fprintln(w, "# HELP xray_bytespool_large_frees_total bytespool large slice release count.")
	fmt.Fprintln(w, "# TYPE xray_bytespool_large_frees_total counter")
	fmt.Fprintf(w, "xray_bytespool_large_frees_total %d\n", snapshot.LargeFreeTotal)
}

func writePipeMetrics(w io.Writer) {
	snapshot := pipe.Metrics()

	fmt.Fprintln(w, "# HELP xray_pipe_active Active internal pipe queues.")
	fmt.Fprintln(w, "# TYPE xray_pipe_active gauge")
	fmt.Fprintf(w, "xray_pipe_active %d\n", snapshot.Active)

	fmt.Fprintln(w, "# HELP xray_pipe_created_total Internal pipe queues created.")
	fmt.Fprintln(w, "# TYPE xray_pipe_created_total counter")
	fmt.Fprintf(w, "xray_pipe_created_total %d\n", snapshot.CreatedTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_buffered_bytes Bytes currently queued in internal pipes.")
	fmt.Fprintln(w, "# TYPE xray_pipe_buffered_bytes gauge")
	fmt.Fprintf(w, "xray_pipe_buffered_bytes %d\n", snapshot.BufferedBytes)

	fmt.Fprintln(w, "# HELP xray_pipe_buffered_buffers buf.Buffer chunks currently queued in internal pipes.")
	fmt.Fprintln(w, "# TYPE xray_pipe_buffered_buffers gauge")
	fmt.Fprintf(w, "xray_pipe_buffered_buffers %d\n", snapshot.BufferedBuffers)

	fmt.Fprintln(w, "# HELP xray_pipe_max_buffered_bytes High-water mark for bytes queued across all internal pipes.")
	fmt.Fprintln(w, "# TYPE xray_pipe_max_buffered_bytes gauge")
	fmt.Fprintf(w, "xray_pipe_max_buffered_bytes %d\n", snapshot.MaxBufferedBytes)

	fmt.Fprintln(w, "# HELP xray_pipe_max_pipe_buffered_bytes High-water mark for bytes queued in a single internal pipe.")
	fmt.Fprintln(w, "# TYPE xray_pipe_max_pipe_buffered_bytes gauge")
	fmt.Fprintf(w, "xray_pipe_max_pipe_buffered_bytes %d\n", snapshot.MaxPipeBufferedBytes)

	fmt.Fprintln(w, "# HELP xray_pipe_writes_total Internal pipe write count.")
	fmt.Fprintln(w, "# TYPE xray_pipe_writes_total counter")
	fmt.Fprintf(w, "xray_pipe_writes_total %d\n", snapshot.WritesTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_write_bytes_total Bytes written into internal pipes.")
	fmt.Fprintln(w, "# TYPE xray_pipe_write_bytes_total counter")
	fmt.Fprintf(w, "xray_pipe_write_bytes_total %d\n", snapshot.WriteBytesTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_reads_total Internal pipe read count.")
	fmt.Fprintln(w, "# TYPE xray_pipe_reads_total counter")
	fmt.Fprintf(w, "xray_pipe_reads_total %d\n", snapshot.ReadsTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_read_bytes_total Bytes read from internal pipes.")
	fmt.Fprintln(w, "# TYPE xray_pipe_read_bytes_total counter")
	fmt.Fprintf(w, "xray_pipe_read_bytes_total %d\n", snapshot.ReadBytesTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_buffer_full_total Internal pipe writes that hit the configured queue limit.")
	fmt.Fprintln(w, "# TYPE xray_pipe_buffer_full_total counter")
	fmt.Fprintf(w, "xray_pipe_buffer_full_total %d\n", snapshot.BufferFullTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_discard_overflow_total Internal pipe writes discarded because discard-overflow mode was enabled.")
	fmt.Fprintln(w, "# TYPE xray_pipe_discard_overflow_total counter")
	fmt.Fprintf(w, "xray_pipe_discard_overflow_total %d\n", snapshot.DiscardOverflowTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_write_closed_total Internal pipe writes attempted after pipe close.")
	fmt.Fprintln(w, "# TYPE xray_pipe_write_closed_total counter")
	fmt.Fprintf(w, "xray_pipe_write_closed_total %d\n", snapshot.WriteClosedTotal)

	fmt.Fprintln(w, "# HELP xray_pipe_write_error_total Internal pipe write errors not otherwise classified.")
	fmt.Fprintln(w, "# TYPE xray_pipe_write_error_total counter")
	fmt.Fprintf(w, "xray_pipe_write_error_total %d\n", snapshot.WriteErrorTotal)

	writeMetricValues(w, "xray_pipe_created_by_limit_total", "Internal pipe queues created by configured byte limit.", "counter", []string{"limit_bytes"}, snapshot.CreatedByLimit)
	writeMetricValues(w, "xray_pipe_active_by_limit", "Active internal pipe queues by configured byte limit.", "gauge", []string{"limit_bytes"}, snapshot.ActiveByLimit)
	writeMetricValues(w, "xray_pipe_queue_bytes_observed_total", "Cumulative observed internal pipe queue size after writes.", "counter", []string{"le"}, snapshot.QueueBytesBuckets)
}

func writeStatsMetrics(w io.Writer, manager feature_stats.Manager) {
	statsManager, ok := manager.(*appstats.Manager)
	if !ok {
		return
	}

	fmt.Fprintln(w, "# HELP xray_stats_traffic_bytes_total Xray built-in traffic counters.")
	fmt.Fprintln(w, "# TYPE xray_stats_traffic_bytes_total counter")
	statsManager.VisitCounters(func(name string, counter feature_stats.Counter) bool {
		parts := strings.Split(name, ">>>")
		if len(parts) < 4 || parts[2] != "traffic" {
			return true
		}
		fmt.Fprintf(
			w,
			"xray_stats_traffic_bytes_total{kind=%s,tag=%s,direction=%s} %d\n",
			quoteLabel(parts[0]),
			quoteLabel(parts[1]),
			quoteLabel(parts[3]),
			counter.Value(),
		)
		return true
	})
}

func writeObservatoryMetrics(w io.Writer, observatoryFeature extension.Observatory) {
	if observatoryFeature == nil {
		return
	}

	report, err := observatoryFeature.GetObservation(context.Background())
	if err != nil {
		return
	}
	result, ok := report.(*observatory.ObservationResult)
	if !ok {
		return
	}

	fmt.Fprintln(w, "# HELP xray_observatory_alive Outbound observatory liveness.")
	fmt.Fprintln(w, "# TYPE xray_observatory_alive gauge")
	fmt.Fprintln(w, "# HELP xray_observatory_delay_milliseconds Outbound observatory delay.")
	fmt.Fprintln(w, "# TYPE xray_observatory_delay_milliseconds gauge")
	fmt.Fprintln(w, "# HELP xray_observatory_health_checks_total Outbound observatory health checks.")
	fmt.Fprintln(w, "# TYPE xray_observatory_health_checks_total counter")
	fmt.Fprintln(w, "# HELP xray_observatory_health_check_failures_total Outbound observatory health check failures.")
	fmt.Fprintln(w, "# TYPE xray_observatory_health_check_failures_total counter")

	for _, status := range result.GetStatus() {
		tag := quoteLabel(status.GetOutboundTag())
		alive := 0
		if status.GetAlive() {
			alive = 1
		}
		fmt.Fprintf(w, "xray_observatory_alive{outbound=%s} %d\n", tag, alive)
		fmt.Fprintf(w, "xray_observatory_delay_milliseconds{outbound=%s} %d\n", tag, status.GetDelay())
		if hp := status.GetHealthPing(); hp != nil {
			fmt.Fprintf(w, "xray_observatory_health_checks_total{outbound=%s} %d\n", tag, hp.GetAll())
			fmt.Fprintf(w, "xray_observatory_health_check_failures_total{outbound=%s} %d\n", tag, hp.GetFail())
		}
	}
}

func addMetric(store *sync.Map, key string, delta int64) {
	value, _ := store.LoadOrStore(key, &atomic.Int64{})
	value.(*atomic.Int64).Add(delta)
}

func observeDuration(bucketStore *sync.Map, sumStore *sync.Map, countStore *sync.Map, labels []string, duration time.Duration) {
	seconds := duration.Seconds()
	for _, bucket := range durationBuckets {
		if seconds <= bucket {
			addMetric(bucketStore, labelsKey(append(labels, strconv.FormatFloat(bucket, 'f', -1, 64))...), 1)
		}
	}
	addMetric(bucketStore, labelsKey(append(labels, "+Inf")...), 1)
	addMetric(sumStore, labelsKey(labels...), int64(duration/time.Microsecond))
	addMetric(countStore, labelsKey(labels...), 1)
}

func writeGaugeVec(w io.Writer, name string, help string, labelNames []string, store *sync.Map) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	writeVecSamples(w, name, labelNames, store, false)
}

func writeCounterVec(w io.Writer, name string, help string, labelNames []string, store *sync.Map) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	writeVecSamples(w, name, labelNames, store, false)
}

func writeHistogram(w io.Writer, name string, help string, labelNames []string, buckets *sync.Map, sums *sync.Map, counts *sync.Map) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	writeVecSamples(w, name+"_bucket", append(labelNames, "le"), buckets, false)
	writeVecSamples(w, name+"_sum", labelNames, sums, true)
	writeVecSamples(w, name+"_count", labelNames, counts, false)
}

func writeVecSamples(w io.Writer, name string, labelNames []string, store *sync.Map, secondsFromMicros bool) {
	keys := make([]string, 0)
	store.Range(func(key interface{}, _ interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	sort.Strings(keys)

	for _, key := range keys {
		value, _ := store.Load(key)
		metricValue := value.(*atomic.Int64).Load()
		labels := strings.Split(key, "\xff")
		if secondsFromMicros {
			fmt.Fprintf(w, "%s%s %.6f\n", name, formatLabels(labelNames, labels), float64(metricValue)/1_000_000)
		} else {
			fmt.Fprintf(w, "%s%s %d\n", name, formatLabels(labelNames, labels), metricValue)
		}
	}
}

func writeMetricValues(w io.Writer, name string, help string, metricType string, labelNames []string, values []pipe.MetricValue) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
	for _, value := range values {
		fmt.Fprintf(w, "%s%s %d\n", name, formatLabels(labelNames, value.Labels), value.Value)
	}
}

type procSelfStatm struct {
	sizeBytes     uint64
	residentBytes uint64
	sharedBytes   uint64
	dataBytes     uint64
}

func readProcSelfStatm() (procSelfStatm, bool) {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return procSelfStatm{}, false
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 6 {
		return procSelfStatm{}, false
	}

	sizePages, ok := parseUintField(fields[0])
	if !ok {
		return procSelfStatm{}, false
	}
	residentPages, ok := parseUintField(fields[1])
	if !ok {
		return procSelfStatm{}, false
	}
	sharedPages, ok := parseUintField(fields[2])
	if !ok {
		return procSelfStatm{}, false
	}
	dataPages, ok := parseUintField(fields[5])
	if !ok {
		return procSelfStatm{}, false
	}

	pageSize := uint64(os.Getpagesize())
	return procSelfStatm{
		sizeBytes:     sizePages * pageSize,
		residentBytes: residentPages * pageSize,
		sharedBytes:   sharedPages * pageSize,
		dataBytes:     dataPages * pageSize,
	}, true
}

func readUintFile(path string) (int64, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value := strings.TrimSpace(string(raw))
	if value == "max" {
		return -1, true
	}
	parsed, ok := parseUintField(value)
	if !ok {
		return 0, false
	}
	if parsed > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int64(parsed), true
}

func parseUintField(value string) (uint64, bool) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func labelsKey(labels ...string) string {
	for i := range labels {
		labels[i] = normalizeLabel(labels[i], "unknown")
	}
	return strings.Join(labels, "\xff")
}

func formatLabels(names []string, values []string) string {
	if len(names) == 0 {
		return ""
	}

	parts := make([]string, 0, len(names))
	for i, name := range names {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		parts = append(parts, name+"="+quoteLabel(value))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func quoteLabel(value string) string {
	return strconv.Quote(value)
}

func normalizeLabel(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

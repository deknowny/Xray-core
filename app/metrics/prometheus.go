package metrics

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	appstats "github.com/xtls/xray-core/app/stats"
	xerrors "github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/extension"
	feature_stats "github.com/xtls/xray-core/features/stats"
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

	fmt.Fprintln(w, "# HELP xray_runtime_heap_objects Go heap object count.")
	fmt.Fprintln(w, "# TYPE xray_runtime_heap_objects gauge")
	fmt.Fprintf(w, "xray_runtime_heap_objects %d\n", mem.HeapObjects)

	fmt.Fprintln(w, "# HELP xray_runtime_gc_total Go GC cycles.")
	fmt.Fprintln(w, "# TYPE xray_runtime_gc_total counter")
	fmt.Fprintf(w, "xray_runtime_gc_total %d\n", mem.NumGC)

	fmt.Fprintln(w, "# HELP xray_runtime_gc_pause_seconds_total Total Go GC pause time.")
	fmt.Fprintln(w, "# TYPE xray_runtime_gc_pause_seconds_total counter")
	fmt.Fprintf(w, "xray_runtime_gc_pause_seconds_total %.6f\n", float64(mem.PauseTotalNs)/float64(time.Second))
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

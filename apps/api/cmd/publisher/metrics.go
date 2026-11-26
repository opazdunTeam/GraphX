package main

import (
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type publisherMetrics struct {
	published uint64
	deferred  uint64
	failed    uint64
	registry  *prometheus.Registry
	total     *prometheus.CounterVec
	duration  *prometheus.HistogramVec
}

func newPublisherMetrics() (*publisherMetrics, error) {
	registry := prometheus.NewRegistry()
	total := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "graphx_outbox_publish_total",
		Help: "Total outbox publish attempts by outcome.",
	}, []string{"outcome"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "graphx_outbox_publish_duration_seconds",
		Help:    "Time spent processing an outbox message.",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"})
	if err := registry.Register(total); err != nil {
		return nil, err
	}
	if err := registry.Register(duration); err != nil {
		return nil, err
	}
	return &publisherMetrics{
		registry: registry,
		total:    total,
		duration: duration,
	}, nil
}

func (m *publisherMetrics) record(
	outcome string,
	elapsed time.Duration,
	result outbox.PublishResult,
) {
	switch outcome {
	case "published", "deferred", "failed":
	default:
		return
	}
	counter := m.total.WithLabelValues(outcome)
	if labels, ok := publisherMetricExemplar(result); ok {
		if exemplarCounter, ok := counter.(prometheus.ExemplarAdder); ok {
			exemplarCounter.AddWithExemplar(1, labels)
		} else {
			counter.Inc()
		}
	} else {
		counter.Inc()
	}
	m.duration.WithLabelValues(outcome).Observe(elapsed.Seconds())
}

func publisherMetricExemplar(result outbox.PublishResult) (prometheus.Labels, bool) {
	investigationID, investigationErr := uuid.Parse(result.InvestigationID)
	jobID, jobErr := uuid.Parse(result.JobID)
	messageID, messageErr := uuid.Parse(result.MessageID)
	if investigationErr != nil || jobErr != nil || messageErr != nil {
		return nil, false
	}
	return prometheus.Labels{
		"inv_id": investigationID.String(),
		"job_id": jobID.String(),
		"msg_id": messageID.String(),
	}, true
}

func (m *publisherMetrics) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(
		m.registry,
		promhttp.HandlerOpts{EnableOpenMetrics: true},
	))
	return mux
}

func startPublisherMetricsServer(
	address string,
	metrics *publisherMetrics,
) (*http.Server, <-chan error, error) {
	if metrics == nil || metrics.registry == nil {
		return nil, nil, errors.New("publisher metrics are required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, err
	}
	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           metrics.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()
	return server, serveErrors, nil
}

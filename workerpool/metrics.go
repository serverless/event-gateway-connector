package workerpool

import (
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(messagesProcessed)
	prometheus.MustRegister(bytesProcessed)
}

var messagesProcessed = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "eventgateway",
		Subsystem: "connector",
		Name:      "processed_total",
		Help:      "Total of processed messages.",
	}, []string{"space", "connectionID"})

var bytesProcessed = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "eventgateway",
		Subsystem: "connector",
		Name:      "processed_bytes",
		Help:      "Total of processed bytes.",
	}, []string{"space", "connectionID"})

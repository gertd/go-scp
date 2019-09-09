package ingest

import (
	"sync/atomic"

	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

// EventBatch -- batched collection of events which fit within space and time constraints of target
type EventBatch struct {
	events    []ingest.Event
	batchSize int64
}

// NewEventBatch --
func NewEventBatch() *EventBatch {
	return &EventBatch{
		events: make([]ingest.Event, 0),
	}
}

// Add -- adds events to batch
func (b *EventBatch) Add(e ingest.Event, eventSize int64) {
	b.events = append(b.events, e)
	atomic.AddInt64(&b.batchSize, eventSize)
}

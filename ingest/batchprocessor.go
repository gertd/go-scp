package ingest

import (
	"encoding/json"
	"log"
	"math"
	"sync/atomic"
	"time"

	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

const (
	batchWindow   = time.Duration(time.Second * 3) // default time window for filling up batch
	maxBatchSize  = int64(1024 * 1024)             // max request size (excl overhead, see https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
	maxEventCount = int(500)                       // max number of events in batch https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
)

// BatchProcessor -- consumer-producer, consumes events, produces batches
type BatchProcessor struct {
	events        <-chan ingest.Event
	batches       chan EventBatch
	quit          chan bool
	totalEvents   int64
	totalByteSize int64
	totalTime     time.Duration
}

// NewBatchProcessor --
func NewBatchProcessor(events <-chan ingest.Event, quit chan bool) *BatchProcessor {
	return &BatchProcessor{
		events:  events,
		batches: make(chan EventBatch),
		quit:    quit,
	}
}

// Batches -- outgoing batch event channel
func (bp *BatchProcessor) Batches() <-chan EventBatch {
	return bp.batches
}

// Run -- execute batch processor
func (bp *BatchProcessor) Run() {

	defer close(bp.batches)

	ticker := time.Tick(batchWindow)

	batch := NewEventBatch()

	for {
		select {
		case e, ok := <-bp.events:
			if ok {

				eventSize := eventSize(e)

				atomic.AddInt64(&bp.totalEvents, int64(1))
				atomic.AddInt64(&bp.totalByteSize, eventSize)

				if batch.batchSize+eventSize < maxBatchSize && len(batch.events) < maxEventCount {
					batch.Add(e, eventSize)
				} else {
					log.Printf("BatchProcessor batch size induced batch send count %d size %d", len(batch.events), batch.batchSize)
					bp.batches <- *batch

					batch = NewEventBatch()
					batch.Add(e, eventSize)
				}
			} else {
				log.Printf("BatchProcessor events channel closed")
				return
			}

		case <-ticker:
			if len(batch.events) > 0 {
				log.Printf("BatchProcessor timer induced batch send count %d size %d", len(batch.events), batch.batchSize)
				bp.batches <- *batch
				batch = NewEventBatch()
			}

		case <-bp.quit:
			if len(batch.events) > 0 {
				log.Printf("BatchProcessor done induced batch send count %d size %d", len(batch.events), batch.batchSize)
				bp.batches <- *batch
				batch = NewEventBatch()
			}
			return
		}
	}
}

// TotalEvents --
func (bp *BatchProcessor) TotalEvents() int64 {
	return bp.totalEvents
}

// TotalByteSize --
func (bp *BatchProcessor) TotalByteSize() int64 {
	return bp.totalByteSize
}

// eventSize -- determine byte size of ingest.Event instace, returns math.MaxInt64 on error
func eventSize(e ingest.Event) int64 {
	if b, err := json.Marshal(e); err != nil {
		return math.MaxInt64
	} else {
		return int64(len(b))
	}
}

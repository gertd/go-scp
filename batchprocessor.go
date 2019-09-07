package scp

import (
	"encoding/json"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

const (
	batchWindow   = time.Duration(time.Second * 3) // default time window for filling up batch
	maxBatchSize  = int64(1024 * 1024)             // max request size (excl overhead, see https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
	maxEventCount = int(500)                       // max number of events in batch https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
)

// BatchProcessor --
type BatchProcessor struct {
	client           *Client
	totalEvents      int64
	totalBatches     int64
	totalByteSize    int64
	totalTime        time.Duration
	eventChannel     chan Event
	batchChannel     chan EventBatch
	doneChannel      chan bool
	batch            *EventBatch
	lastBatchWritten int64
}

// NewBatchProcessor --
func NewBatchProcessor(client *Client) *BatchProcessor {
	return &BatchProcessor{
		client:       client,
		eventChannel: make(chan Event),
		batchChannel: make(chan EventBatch),
		doneChannel:  make(chan bool),
		batch:        NewEventBatch(),
	}
}

// Start --
func (bp *BatchProcessor) Start() {
	go bp.batchWriter()
	go bp.eventHandler()
}

// Close --
func (bp *BatchProcessor) Close() {
	bp.doneChannel <- true
	close(bp.eventChannel)
	for bp.totalBatches != bp.lastBatchWritten {
		time.Sleep(time.Duration(time.Second * 1))
	}
	log.Printf("close total %d last %d", bp.totalBatches, bp.lastBatchWritten)
	close(bp.batchChannel)
}

// TotalEvents --
func (bp *BatchProcessor) TotalEvents() int64 {
	return bp.totalEvents
}

// TotalBatches --
func (bp *BatchProcessor) TotalBatches() int64 {
	return bp.totalBatches
}

// TotalByteSize --
func (bp *BatchProcessor) TotalByteSize() int64 {
	return bp.totalByteSize
}

// Event --
type Event struct {
	event     ingest.Event
	eventSize int64
}

// Size -- event payload size in bytes
func (e *Event) Size() int64 {
	if e.eventSize == 0 {
		b, _ := json.Marshal(e.event)
		e.eventSize = int64(len(b))
	}
	return e.eventSize
}

// Size2 -- alternative event size implementation
// func (e *Event) Size2() int64 {

// 	var eventbody = e.event.Body

// 	bodystring, ok := eventbody.(string)
// 	if ok {
// 		return int64(len(bodystring))

// 	}
// 	bodyNum, ok := eventbody.(int64)
// 	if ok {
// 		return int64(len(string(bodyNum)))
// 	}

// 	bodyNum1, ok := eventbody.(int32)
// 	if ok {
// 		return int64(len(string(bodyNum1)))
// 	}

// 	bodyNum2, ok := eventbody.(int)
// 	if ok {
// 		return int64(len(string(bodyNum2)))
// 	}

// 	bodyMap, ok := eventbody.(map[string]interface{})
// 	if ok {
// 		for _, bodyval := range bodyMap {
// 			return int64(len(bodyval.(string)))
// 		}
// 	}

// 	_, ok = eventbody.(bool)
// 	if ok {
// 		return int64(1)
// 	}

// 	// return the max value so that all previous events can be flushed
// 	return math.MaxInt64 //, errors.New("can't read event, the event type is not supported")

// }

// EventBatch --
type EventBatch struct {
	events     []ingest.Event
	batchSize  int64
	// batchSize2 int64
}

// NewEventBatch --
func NewEventBatch() *EventBatch {
	return &EventBatch{
		events: make([]ingest.Event, 0),
	}
}

func (eb *EventBatch) add(e Event) {
	eb.events = append(eb.events, e.event)
	atomic.AddInt64(&eb.batchSize, e.Size())
	// atomic.AddInt64(&eb.batchSize2, e.Size2())
}

// Send --
func (bp *BatchProcessor) Send(e ingest.Event) error {

	// ee := Event{
	// 	event: e,
	// }

	bp.eventChannel <- Event{event: e}

	return nil
}

// eventHandler -- go routine which handles incoming events of eventChannel
func (bp *BatchProcessor) eventHandler() {
	log.Printf("eventHandler")
	defer bp.Close()
	defer log.Printf("eventHandler closed")

	bp.batch = NewEventBatch()

	ticker := time.Tick(batchWindow)

	for {
		select {
		case e, ok := <-bp.eventChannel:
			if ok {

				atomic.AddInt64(&bp.totalEvents, int64(1))
				atomic.AddInt64(&bp.totalByteSize, e.Size())

				if bp.batch.batchSize+e.Size() < maxBatchSize && len(bp.batch.events) < maxEventCount {
					bp.batch.add(e)
				} else {
					log.Printf("batch overflow induced batch send count %d size %d", len(bp.batch.events), bp.batch.batchSize)
					bp.batchChannel <- *bp.batch
					bp.batch = NewEventBatch()
					bp.batch.add(e)
				}
			}

		case <-ticker:
			if len(bp.batch.events) > 0 {
				log.Printf("timer induced batch send count %d size %d", len(bp.batch.events), bp.batch.batchSize)
				bp.batchChannel <- *bp.batch
				bp.batch = NewEventBatch()
			}

		case <-bp.doneChannel:
			if len(bp.batch.events) > 0 {
				log.Printf("done induced batch send count %d size %d", len(bp.batch.events), bp.batch.batchSize)
				bp.batchChannel <- *bp.batch
				bp.batch = NewEventBatch()
			}
			return
		}
	}
}

// batchWriter -- go routine which writes out the batches received on batchChannel
func (bp *BatchProcessor) batchWriter() {
	log.Printf("batchWriter")
	defer log.Printf("batchWriter closed")

	for {
		select {
		case b, ok := <-bp.batchChannel:
			if ok {
				atomic.AddInt64(&bp.totalBatches, int64(1))
				log.Printf("batchWriter count %d size %d", len(b.events), b.batchSize)

				if err := bp.client.IngestEvents(&bp.batch.events); err != nil {
					log.Printf("error ingest events %s", err.Error())
				}

				atomic.SwapInt64(&bp.lastBatchWritten, bp.totalBatches)
			} else {
				log.Printf("batchWriter break count %d size %d", len(b.events), b.batchSize)
				break
			}
		}
	}
}

// EventProducerJSON --
func EventProducerJSON(r io.Reader, sender func(ingest.Event) error) error {

	dec := json.NewDecoder(r)

	var e interface{}

	for dec.More() {

		if err := dec.Decode(&e); err != nil {
			return err
		}

		// TODO refactor into reducer function
		// if m, ok := e.(map[string]interface{}); ok {
		// 	if _, ok := m["index"]; ok {
		// 		continue
		// 	}
		// }

		ts := time.Now().UTC().Unix() * 1000
		ns := int32(0)

		event := ingest.Event{
			// Host:       &b.config.hostname,
			// Source:     &b.config.source,
			// Sourcetype: &b.config.sourcetype,
			Timestamp: &ts,
			Nanos:     &ns,
			Body:      e,
		}

		if err := sender(event); err != nil {
			return err
		}
	}
	return nil
}

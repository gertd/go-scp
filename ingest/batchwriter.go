package ingest

import (
	"log"
	"sync/atomic"

	"github.com/gertd/go-scp/scp"
)

// BatchWriter -- writes batch of events to target
type BatchWriter struct {
	client       *scp.Client
	batches      <-chan EventBatch
	quit         chan bool
	totalBatches int64
}

// NewBatchWriter --
func NewBatchWriter(client *scp.Client, batches <-chan EventBatch, quit chan bool) *BatchWriter {
	return &BatchWriter{
		client:  client,
		batches: batches,
		quit:    quit,
	}
}

// Run -- execute batch writer
func (bw *BatchWriter) Run() {

	for {
		select {
		case b, ok := <-bw.batches:
			if ok {
				log.Printf("BatchWriter write batch %d size %d", len(b.events), b.batchSize)
				if err := bw.client.IngestEvents(&b.events); err != nil {
					log.Printf("BatchWriter error IngestEvents %s", err.Error())
					bw.quit <- true
					return
				}
				atomic.AddInt64(&bw.totalBatches, int64(1))
			} else {
				log.Printf("BatchWriter batches channel closed")
				return
			}
		case <-bw.quit:
			return
		}
	}
}

// TotalBatches --
func (bw *BatchWriter) TotalBatches() int64 {
	return bw.totalBatches
}

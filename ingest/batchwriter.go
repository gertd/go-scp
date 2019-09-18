package ingest

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/gertd/go-scp/scp"
)

// BatchWriter -- writes batch of events to target
type BatchWriter struct {
	ctx          context.Context
	client       *scp.Client
	batches      <-chan EventBatch
	totalBatches int64
}

// NewBatchWriter --
func NewBatchWriter(ctx context.Context, client *scp.Client, batches <-chan EventBatch) *BatchWriter {
	return &BatchWriter{
		ctx:     ctx,
		client:  client,
		batches: batches,
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
					_, cancel := context.WithCancel(bw.ctx)
					cancel()
					return
				}
				atomic.AddInt64(&bw.totalBatches, int64(1))
			} else {
				log.Printf("BatchWriter batches channel closed")
				return
			}
		case <-bw.ctx.Done():
			return
		}
	}
}

// TotalBatches --
func (bw *BatchWriter) TotalBatches() int64 {
	return bw.totalBatches
}

package csv

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"time"

	"github.com/gertd/go-scp/events"
	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

// Producer -- event producer
type producer struct {
	ctx    context.Context
	events chan ingest.Event
}

// NewEventsProducer -- construct new CSV even producer
func NewEventsProducer(ctx context.Context) events.Producer {

	return producer{
		ctx:    ctx,
		events: make(chan ingest.Event),
	}
}

// Events -- outgoing event channel
func (ep producer) Events() <-chan ingest.Event {
	return ep.events
}

// Run --
func (ep producer) Run(r io.Reader, p events.Properties) {

	defer close(ep.events)

	rdr := csv.NewReader(r)
	rdr.ReuseRecord = true

	record, err := rdr.Read()
	if err != nil {
		log.Fatal(err)
	}

	header := []string{}
	header = append(header, record...)

	for {

		record, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		body := make(map[string]interface{})
		for i, field := range record {
			body[header[i]] = field
		}

		ts := time.Now().UTC().Unix() * 1000
		ns := int32(0)

		event := ingest.Event{
			Host:       p.Host,
			Source:     p.Source,
			Sourcetype: p.Sourcetype,
			Timestamp:  &ts,
			Nanos:      &ns,
			Body:       body,
		}

		select {
		case ep.events <- event:
		case <-ep.ctx.Done():
			return
		}
	}
	return
}

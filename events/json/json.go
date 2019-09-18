package json

import (
	"context"
	"encoding/json"
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

// NewEventsProducer -- construct new JSON even producer
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

	dec := json.NewDecoder(r)

	var body interface{}

	for dec.More() {

		if err := dec.Decode(&body); err != nil {
			log.Printf("error decode %v", err)
			return
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

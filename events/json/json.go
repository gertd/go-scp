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
	props  events.Properties
	reader io.Reader
}

// NewEventsProducer -- construct new JSON even producer
func NewEventsProducer(ctx context.Context, r io.Reader, p events.Properties) events.Producer {

	return producer{
		ctx:    ctx,
		events: make(chan ingest.Event),
		props:  p,
		reader: r,
	}
}

// Events -- outgoing event channel
func (ep producer) Events() <-chan ingest.Event {
	return ep.events
}

// Run --
func (ep producer) Run() {

	defer close(ep.events)

	dec := json.NewDecoder(ep.reader)

	var body interface{}

	for dec.More() {

		if err := dec.Decode(&body); err != nil {
			log.Printf("error decode %v", err)
			return
		}

		ts := time.Now().UTC().Unix() * 1000
		ns := int32(0)

		event := ingest.Event{
			Host:       ep.props.Host,
			Source:     ep.props.Source,
			Sourcetype: ep.props.Sourcetype,
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

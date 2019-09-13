package json

import (
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
	"gitlab.com/d5s/go-scp/events"
)

// Producer -- event producer
type producer struct {
	events chan ingest.Event
	quit   chan bool
}

// NewEventsProducer -- construct new JSON even producer
func NewEventsProducer(quit chan bool) events.Producer {

	return producer{
		events: make(chan ingest.Event),
		quit:   quit,
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
		case <-ep.quit:
			return
		}
	}
	return
}

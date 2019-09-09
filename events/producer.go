package events

import (
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

// Producer --
type Producer struct {
	events chan ingest.Event
	quit   chan bool
}

// Events -- outgoing event channel
func (ep *Producer) Events() <-chan ingest.Event {
	return ep.events
}

// NewEventsProducerJSON -- construct new JSON even producer
func NewEventsProducerJSON(quit chan bool) *Producer {

	return &Producer{
		events: make(chan ingest.Event),
		quit:   quit,
	}
}

// Run --
func (ep *Producer) Run(r io.Reader) {

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
			// Host:       &b.config.hostname,
			// Source:     &b.config.source,
			// Sourcetype: &b.config.sourcetype,
			Timestamp: &ts,
			Nanos:     &ns,
			Body:      body,
		}

		select {
		case ep.events <- event:
		case <-ep.quit:
			return
		}
	}
	return
}

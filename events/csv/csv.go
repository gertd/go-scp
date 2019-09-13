package csv

import (
	"encoding/csv"
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

// NewEventsProducer -- construct new CSV even producer
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
		case <-ep.quit:
			return
		}
	}
	return
}

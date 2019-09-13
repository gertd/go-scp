package events

import (
	"io"

	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

// Producer -- event producer interface
type Producer interface {
	Events() <-chan ingest.Event
	Run(r io.Reader, p Properties)
}

// Properties -- settabble event properties
type Properties struct {
	Host       *string
	Source     *string
	Sourcetype *string
}

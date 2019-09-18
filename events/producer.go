package events

import (
	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

// Producer -- event producer interface
type Producer interface {
	Events() <-chan ingest.Event
	Run()
}

// Properties -- settabble event properties
type Properties struct {
	Host       *string
	Source     *string
	Sourcetype *string
}

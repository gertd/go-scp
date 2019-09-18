package scp

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/splunk/splunk-cloud-sdk-go/idp"
	"github.com/splunk/splunk-cloud-sdk-go/services/ingest"
)

// IngestEvent --
func (c *Client) IngestEvent(e *ingest.Event) error {

	if e == nil {
		return fmt.Errorf("e nil pointer")
	}

	*e.Timestamp = time.Now().UTC().Unix() * 1000

	events := []ingest.Event{*e}

	return c.IngestEvents(&events)
}

// IngestEvents --
func (c *Client) IngestEvents(events *[]ingest.Event) error {
	defer duration(track("IngestEvents"))

	if events == nil {
		return fmt.Errorf("events nil pointer")
	}

	token, err := c.TokenSource.Token()
	if err != nil {
		return err
	}

	c.Service.UpdateTokenContext(&idp.Context{
		AccessToken: token.AccessToken,
	})

	return retry(3, time.Second, func() error {

		var resp http.Response

		_, err := c.Service.IngestService.PostEvents(*events, &resp)
		if err != nil {
			log.Printf("PostEvents %s (%d)\n", resp.Status, resp.StatusCode)
		}

		s := resp.StatusCode
		switch {
		case s == 429: // Retry rate limited
			return fmt.Errorf("server error: %d", s)
		case s >= 500: // Retry server side failures
			return fmt.Errorf("server error: %d", s)
		case s >= 400: // Don't retry client failures (expect 429)
			return stop{fmt.Errorf("client error: %d", s)}
		default:
			return nil
		}
	})
}

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	log.Printf("%s: %s\n", msg, time.Since(start))
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if s, ok := err.(stop); ok {
			// Return the original error for later checking
			return s.error
		}

		if attempts--; attempts > 0 {
			// Add some randomness to prevent creating a Thundering Herd
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2

			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}

type stop struct {
	error
}

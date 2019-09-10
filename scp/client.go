package scp

import (
	"context"

	"github.com/splunk/splunk-cloud-sdk-go/sdk"
	"github.com/splunk/splunk-cloud-sdk-go/services"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	tokenURL = "https://auth.scp.splunk.com/token" /* #nosec */
)

// Client --
type Client struct {
	Tenant      string
	TokenSource oauth2.TokenSource
	Service     *sdk.Client
}

// NewClient -- create SCP client using TokenSource (client ID & secret)
func NewClient(tenant, clientID, clientSecret string) *Client {

	clientConfig := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
		Scopes:       []string{"client_credentials"},
	}

	c := Client{
		Tenant:      tenant,
		TokenSource: clientConfig.TokenSource(context.Background()),
	}

	return &c
}

// Authenticate --
func (c *Client) Authenticate() error {

	token, err := c.TokenSource.Token()
	if err != nil {
		return err
	}

	c.Service, err = sdk.NewClient(&services.Config{
		Tenant: c.Tenant,
		Token:  token.AccessToken,
	})
	if err != nil {
		return err
	}
	return nil
}

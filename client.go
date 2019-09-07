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
	TokenSource oauth2.TokenSource
	Service     *sdk.Client
}

// NewClient -- create SCP client using client id & secret
func NewClient(tenant, clientID, clientSecret string) *Client {

	clientConfig := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
		Scopes:       []string{"client_credentials"},
	}

	c := Client{
		TokenSource: clientConfig.TokenSource(context.Background()),
	}

	token, _ := c.TokenSource.Token()

	// tr := idp.TokenRetriever{}
	// ctx := idp.Context{}

	c.Service, _ = sdk.NewClient(&services.Config{
		//TokenRetriever: tr,
		Tenant: tenant,
		Token:  token.AccessToken,
	})

	return &c
}

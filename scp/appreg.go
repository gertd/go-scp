package scp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

// AppReg -- application registration result
type AppReg struct {
	ClientID                string    `json:"clientId"`
	ClientSecret            string    `json:"clientSecret"`
	CreatedAt               time.Time `json:"createdAt"`
	CreatedBy               string    `json:"createdBy"`
	Kind                    string    `json:"kind"`
	Name                    string    `json:"name"`
	Title                   string    `json:"title"`
	AppPrincipalPermissions []string  `json:"appPrincipalPermissions"`
	Description             string    `json:"description"`
	LoginURL                string    `json:"loginUrl"`
	LogoURL                 string    `json:"logoUrl,omitempty"`
	RedirectUrls            []string  `json:"redirectUrls"`
	SetupURL                string    `json:"setupUrl,omitempty"`
	UserPermissionsFilter   []string  `json:"userPermissionsFilter"`
	WebhookURL              string    `json:"webhookUrl,omitempty"`
}

// Load --
func (a *AppReg) Load(filename string) error {

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return fmt.Errorf("file [%s] does not exist", filename)
	}

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, a)
	if err != nil {
		return err
	}
	return nil
}

package sacmclient

import (
	"crypto/sha512"
	"hash"

	"github.com/xdg/scram"
)

// Reference: https://github.com/Shopify/sarama/blob/master/examples/sasl_scram_client/scram_client.go

// SHA512 is the hash generator
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

// XDGSCRAMClient represents the SAML/SCRAM client
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin begins to generate a conversation
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step

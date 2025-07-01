package tessara

import (
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

// keyDistributeQualifier is an implementation of the qualifier interface that distributes keys evenly across subqueues.
type keyDistributeQualifier struct{}

// newKeyDistributeQualifier creates a new instance of keyDistributeQualifier.
func newKeyDistributeQualifier() keyDistributeQualifier {
	return keyDistributeQualifier{}
}

// Qualify distributes a key evenly across subqueues.
func (k keyDistributeQualifier) Qualify(key string, sqs []*subqueue) *subqueue {
	if key == "" {
		key = uuid.New().String()
	}
	hash := xxhash.Sum64String(key)
	sqsIndex := int(hash % uint64(len(sqs)))
	return sqs[sqsIndex]
}

package tessara

// roundRobinQualifier implements a round-robin subqueue qualifier.
type roundRobinQualifier struct {
	roundRobinIndex int
}

// newRoundRobinQualifier creates a new round-robin qualifier.
func newRoundRobinQualifier() *roundRobinQualifier {
	return &roundRobinQualifier{roundRobinIndex: 0}
}

// Qualify selects a subqueue using round-robin algorithm.
func (r *roundRobinQualifier) Qualify(key string, sqs []*subqueue) *subqueue {
	subqueueQualified := sqs[r.roundRobinIndex]
	r.increaseRoundRobin(len(sqs))
	return subqueueQualified
}

// increaseRoundRobin increases the round-robin index.
func (r *roundRobinQualifier) increaseRoundRobin(subqueueLength int) {
	r.roundRobinIndex = (r.roundRobinIndex + 1) % subqueueLength
}

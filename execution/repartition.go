package execution

import "errors"

// RepartitionValidator is a backend capability required before a topology with
// exchanges opens any task. Topics must have the adapter's internal key-routing
// policy configured. Implementations must treat the supplied list as read-only.
// Backends can omit it to decline exchanges. Transport models explicitly accept
// caller-delivered exchanges; wrappers must forward the capability deliberately.
type RepartitionValidator interface {
	ValidateRepartitionTopics([]string) error
}

func (g *Group) ValidateRepartitionTopics(topics []string) error {
	if g == nil || g.backend == nil {
		return errors.New("uninitialized transaction group")
	}
	if validator, ok := g.backend.(RepartitionValidator); ok {
		return validator.ValidateRepartitionTopics(topics)
	}
	return errors.New("atomic backend does not declare repartition transport support")
}

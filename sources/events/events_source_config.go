package events

import "time"

// KubernetesEventsSourceConfig defines optional filters for events collection.
type KubernetesEventsSourceConfig struct {
	EventTypes       []string       `hcl:"event_types,optional"`
	ObjectKinds      []string       `hcl:"object_kinds,optional"`
	ObjectNames      []string       `hcl:"object_names,optional"`
	SourceComponents []string       `hcl:"source_components,optional"`
	MessagePattern   string         `hcl:"message_pattern,optional"`
	ReasonPattern    string         `hcl:"reason_pattern,optional"`
	Limit            *int64         `hcl:"limit,optional"`
	Watch            bool           `hcl:"watch,optional"`
	WatchTimeout     *time.Duration `hcl:"watch_timeout,optional"`
}

func (c *KubernetesEventsSourceConfig) Identifier() string { return "kubernetes_events" }
func (c *KubernetesEventsSourceConfig) Validate() error    { return nil }

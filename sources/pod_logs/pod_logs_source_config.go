package pod_logs

import "fmt"

// KubernetesPodLogsSourceConfig defines options to filter pod logs at the source level.
// Connection parameters (auth, namespaces, since_time, follow) are taken from the connection config.
type KubernetesPodLogsSourceConfig struct {
	// Optional regex to filter pod names
	PodNamePattern string `hcl:"pod_name_pattern,optional"`
	// Optional log levels to include (INFO, WARN, ERROR, etc.)
	LogLevels []string `hcl:"log_levels,optional"`
	// Optional additional label selectors (ANDed)
	LabelSelectors []string `hcl:"label_selectors,optional"`
	// Optional tail lines and limit bytes
	TailLines  *int64 `hcl:"tail_lines,optional"`
	LimitBytes *int64 `hcl:"limit_bytes,optional"`
}

func (c *KubernetesPodLogsSourceConfig) Identifier() string { return "kubernetes_pod_logs" }

func (c *KubernetesPodLogsSourceConfig) Validate() error {
	if c.TailLines != nil && *c.TailLines < 0 {
		return fmt.Errorf("tail_lines cannot be negative")
	}
	if c.LimitBytes != nil && *c.LimitBytes < 0 {
		return fmt.Errorf("limit_bytes cannot be negative")
	}
	return nil
}

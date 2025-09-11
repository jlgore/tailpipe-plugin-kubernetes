package audit_logs

import (
    "fmt"
)

// KubernetesAuditLogsSourceConfig defines source-level options for collecting Kubernetes audit logs.
// Connection details (kubeconfig, server, namespaces, etc.) are supplied by the connection config.
type KubernetesAuditLogsSourceConfig struct {
    // Method optionally forces a collection path: "file", "apiserver", or "daemonset".
    // If empty, the source will auto-detect in this order: file -> apiserver -> daemonset.
    Method *string `hcl:"method,optional"`

    // AuditLogPaths optionally overrides default file paths searched for audit logs (file method only).
    AuditLogPaths []string `hcl:"audit_log_paths,optional"`

    // ApiOnly, if true, will only use Kubernetes API-based methods (apiserver, daemonset)
    // and skip any direct file access attempts. Useful for Talos or managed control planes.
    ApiOnly bool `hcl:"api_only,optional"`
}

// Identifier implements parse.Config for the source config type.
func (c *KubernetesAuditLogsSourceConfig) Identifier() string {
    return "kubernetes_audit_logs"
}

// Validate implements parse.Config. Performs basic validation on options.
func (c *KubernetesAuditLogsSourceConfig) Validate() error {
    if c.Method == nil || *c.Method == "" {
        return nil
    }
    switch *c.Method {
    case "file", "apiserver", "daemonset":
        return nil
    default:
        return fmt.Errorf("invalid method '%s' - must be one of: file, apiserver, daemonset", *c.Method)
    }
}

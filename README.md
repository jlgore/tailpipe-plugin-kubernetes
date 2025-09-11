# Tailpipe Plugin for Kubernetes

Collect and query Kubernetes cluster logs using SQL with [Tailpipe](https://tailpipe.io)!

## Overview

The Kubernetes plugin for Tailpipe allows you to collect and analyze logs from your Kubernetes clusters using SQL. Query pod logs, cluster events, and resource metadata in real-time or historical batches.

## Features

- **Pod Logs Collection**: Stream logs from containers across pods and namespaces
- **Cluster Events**: Monitor warnings, errors, and state changes
- **Resource Metadata**: Query pods, services, deployments and their configurations
- **Real-time Streaming**: Follow logs in real-time like `kubectl logs -f`
- **Multi-namespace Support**: Collect logs across multiple namespaces
- **Flexible Authentication**: Support for kubeconfig, service accounts, and in-cluster auth

## Quick Start

### Installation

```bash
# Install the plugin (when published)
tailpipe plugin install kubernetes
```

### Configuration

Configure your connection in `~/.tailpipe/config/kubernetes.tpc`:

```hcl
connection "kubernetes" {
  plugin = "kubernetes"
  
  # Use default kubeconfig
  kubeconfig_path = "~/.kube/config"
  
  # Or specify context
  kubeconfig_context = "production-cluster"
  
  # Collection options
  namespace = "*"          # All namespaces
  follow_logs = true       # Real-time streaming
  since_time = "1h"        # Last hour of logs
  include_events = true    # Include cluster events
}
```

### Example Queries

```sql
-- Recent error logs across all pods
SELECT timestamp, namespace, pod_name, log_message
FROM kubernetes_pod_logs
WHERE log_level = 'ERROR'
  AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;

-- Pod restart events
SELECT namespace, object_name, message, count
FROM kubernetes_events
WHERE reason = 'BackOff'
  AND event_type = 'Warning'
ORDER BY last_timestamp DESC;

-- Resource usage by namespace
SELECT namespace, COUNT(*) as pod_count
FROM kubernetes_resources
WHERE resource_type = 'Pod'
GROUP BY namespace;

-- 🔐 Security: Failed authentication attempts (AUDIT LOGS)
SELECT stage_timestamp, user_name, source_ips, request_uri, response_code
FROM kubernetes_audit_logs 
WHERE response_code IN (401, 403) 
  AND stage_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY stage_timestamp DESC;

-- 🔐 Security: Secret access attempts (AUDIT LOGS)
SELECT stage_timestamp, user_name, verb, namespace, name, response_code
FROM kubernetes_audit_logs 
WHERE resource = 'secrets' 
  AND verb IN ('get', 'list')
ORDER BY stage_timestamp DESC;

-- 🔐 Security: Administrative actions (AUDIT LOGS)
SELECT stage_timestamp, user_name, verb, resource, namespace, name
FROM kubernetes_audit_logs 
WHERE verb IN ('create', 'update', 'patch', 'delete')
  AND user_name NOT LIKE 'system:%'
ORDER BY stage_timestamp DESC;
```

## Tables

| Table | Description |
|-------|-------------|
| `kubernetes_pod_logs` | Container logs with metadata and timestamps |
| `kubernetes_events` | Cluster events, warnings, and state changes |
| `kubernetes_resources` | Pods, services, deployments with specs and status |
| `kubernetes_audit_logs` | **NEW!** Kubernetes API server audit logs for security and compliance |

## 🔐 Audit Log Collection

The `kubernetes_audit_logs` table provides comprehensive security monitoring by collecting Kubernetes API server audit logs. This enables:

- **Authentication tracking** - Monitor login attempts, failures, and user activity
- **Authorization monitoring** - Track permission checks and privilege escalations  
- **Resource access auditing** - See who accessed what resources and when
- **Change tracking** - Monitor all create, update, delete operations
- **Compliance reporting** - Generate audit trails for security compliance

### Audit Log Access Methods

The plugin automatically tries multiple methods to access audit logs:

1. **📁 Direct File Access** (most comprehensive)
   - Reads audit log files directly from `/var/log/audit/kube/`
   - Requires the plugin to run with file system access to master nodes
   - Provides complete audit log history

2. **🔍 API Server Pod Logs** (fallback method)  
   - Extracts audit entries from API server container logs
   - Works when audit logs are configured to output to stdout
   - Requires cluster access permissions

3. **📦 DaemonSet Collection** (advanced setup)
   - Collects via dedicated audit log collector pods
   - For environments with custom audit log shipping

You can optionally force a specific method in the source configuration when running a collection:

```hcl
source "kubernetes_audit_logs" {
  # one of: file | apiserver | daemonset
  method = "file"

  # optional override of default file search paths (file method only)
  audit_log_paths = [
    "/var/log/audit/kube/kube-apiserver.log",
    "/var/log/kube-audit/audit.log"
  ]
}
```

### API-only Ingestion (Talos / No SSH)

Use the Kubernetes API to read kube-apiserver container logs (JSON audit events) without any filesystem access:

```hcl
# Connection config (e.g., ~/.tailpipe/config/kubernetes.tpc)
connection "kubernetes" {
  plugin          = "kubernetes"

  # Auth: pick one
  kubeconfig_path = "~/.kube/config"    # or
  # service_account = true               # when running in-cluster

  # Filter by namespaces ("*" for all)
  namespace       = "kube-system,default"

  # Default lookback if collection doesn't specify --from
  since_time      = "15m"

  # Optional: used for row enrichment/identification
  cluster_name    = "talos"
}

# Source config forcing API-only
source "kubernetes_audit_logs" {
  api_only = true         # tries apiserver → daemonset (no file reads)
  # Or force explicitly: method = "apiserver"
}

# RBAC: Tailpipe credentials must be able to read apiserver logs
# - list/get pods in kube-system
# - get pods/log in kube-system

# Ensure kube-apiserver emits JSON audit logs to stdout (so they appear in pod logs):
# - --audit-policy-file=/etc/kubernetes/audit-policy.yaml
# - --audit-log-format=json
# - --audit-log-path=-
```

### Audit Log Configuration

For optimal audit log collection, ensure your cluster has audit logging enabled:

```yaml
# API Server audit policy example
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
- level: Metadata
  resources:
  - group: ""
    resources: ["secrets", "configmaps"]
- level: RequestResponse
  users: ["admin", "cluster-admin"]
- level: Request
  verbs: ["create", "update", "patch", "delete"]
```

### Audit Log Schema

Key fields available in `kubernetes_audit_logs`:

| Field | Description |
|-------|-------------|
| `stage_timestamp` | When the audit event occurred |
| `user_name` | Username who made the request |
| `verb` | API operation (get, list, create, update, delete) |
| `resource` | Kubernetes resource type (pods, secrets, etc.) |
| `namespace` | Resource namespace |
| `request_uri` | Full API request URI |
| `source_ips` | Source IP addresses |
| `response_code` | HTTP response code |
| `audit_id` | Unique audit event identifier |
| `level` | Audit level (Metadata, Request, RequestResponse) |

## Authentication

The plugin supports multiple authentication methods:

1. **Kubeconfig file** (default): Uses `~/.kube/config`
2. **In-cluster service account**: Automatic when running in pods
3. **Direct authentication**: Custom server URL and credentials
4. **Cloud provider auth**: AWS EKS, GKE, AKS integration

## Development

### Prerequisites

- Go 1.21+
- Access to a Kubernetes cluster
- `kubectl` configured and working

### Building

```bash
git clone https://github.com/turbot/tailpipe-plugin-kubernetes
cd tailpipe-plugin-kubernetes
go build
```

### Testing

```bash
go test ./...
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Support

- **Community**: Join the [Tailpipe Slack](https://tailpipe.io/slack)
- **Issues**: Report bugs on [GitHub Issues](https://github.com/turbot/tailpipe-plugin-kubernetes/issues)
- **Documentation**: Visit [Tailpipe Hub](https://hub.tailpipe.io/plugins/turbot/kubernetes)

---

Built with ❤️ by [Turbot HQ](https://turbot.com)

package plugin

import (
	"log/slog"

	"github.com/jlgore/tailpipe-plugin-kubernetes/kubernetes"
	_ "github.com/jlgore/tailpipe-plugin-kubernetes/sources/audit_logs" // register audit logs row source
	_ "github.com/jlgore/tailpipe-plugin-kubernetes/sources/events"     // register events row source
	_ "github.com/jlgore/tailpipe-plugin-kubernetes/sources/pod_logs"   // register pod logs row source
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

// init registers all tables with the table factory
func init() {
	// Register pod logs table
	table.RegisterTable[*kubernetes.PodLogRow, *kubernetes.KubernetesPodLogsTable]()

	// Register events table
	table.RegisterTable[*kubernetes.EventRow, *kubernetes.KubernetesEventsTable]()

	// Register resources table
	table.RegisterTable[*kubernetes.ResourceRow, *kubernetes.KubernetesResourcesTable]()

	// Register audit logs table
	table.RegisterTable[*kubernetes.AuditLogRow, *kubernetes.KubernetesAuditLogsTable]()

	slog.Info("Registered Kubernetes plugin tables",
		"tables", []string{"kubernetes_pod_logs", "kubernetes_events", "kubernetes_resources", "kubernetes_audit_logs"})
}

// KubernetesPlugin implements the TailpipePlugin interface
type KubernetesPlugin struct {
	*plugin.PluginImpl

	// Plugin metadata
	metadata *PluginMetadata
}

// PluginMetadata holds plugin information
type PluginMetadata struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
	Author      string `json:"author"`
	License     string `json:"license"`
}

// NewKubernetesPlugin creates a new Kubernetes plugin instance
func NewKubernetesPlugin() *KubernetesPlugin {
	// The identifier must match the connection plugin name used in .tpc files
	// e.g. connection "kubernetes" { plugin = "kubernetes" }
	impl := plugin.NewPluginImpl("kubernetes")

	return &KubernetesPlugin{
		PluginImpl: &impl,
		metadata: &PluginMetadata{
			Name:        "kubernetes",
			Version:     "1.0.0",
			Description: "Tailpipe plugin for collecting and querying Kubernetes cluster logs, events, and resources",
			Author:      "Turbot HQ, Inc.",
			License:     "Apache 2.0",
		},
	}
}

// NOTE: We rely on plugin.PluginImpl (from the SDK) for all Tailpipe GRPC lifecycle methods
// such as Describe, Collect, InitSource, SourceCollect, Pause/Resume, etc.
// Do not override them here unless customization is required.

// Plugin creates and returns the Kubernetes plugin instance
func Plugin() (plugin.TailpipePlugin, error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic during plugin creation", "error", r)
		}
	}()

	plugin := NewKubernetesPlugin()
	slog.Info("Created Kubernetes plugin", "name", plugin.metadata.Name, "version", plugin.metadata.Version)

	return plugin, nil
}

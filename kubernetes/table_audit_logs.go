package kubernetes

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/turbot/tailpipe-plugin-kubernetes/config"
	"github.com/turbot/tailpipe-plugin-kubernetes/internal"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime"
)

// AuditLogRow represents a single Kubernetes audit log entry
type AuditLogRow struct {
	// Core audit fields (matching Kubernetes audit schema)
	Kind         string     `json:"kind"`                    // "Event"
	APIVersion   string     `json:"api_version"`             // "audit.k8s.io/v1"
	Level        string     `json:"level"`                   // Metadata, Request, RequestResponse, None
	AuditID      string     `json:"audit_id"`                // Unique audit ID
	Stage        string     `json:"stage"`                   // RequestReceived, ResponseStarted, ResponseComplete, Panic
	RequestURI   string     `json:"request_uri"`             // API request URI
	Verb         string     `json:"verb"`                    // get, list, create, update, patch, delete
	
	// User and authentication
	UserName     string     `json:"user_name,omitempty"`     // Username
	UserUID      string     `json:"user_uid,omitempty"`      // User UID
	UserGroups   []string   `json:"user_groups,omitempty"`   // User groups
	Impersonated bool       `json:"impersonated"`            // Whether user was impersonated
	
	// Resource information  
	Namespace    string     `json:"namespace,omitempty"`     // Resource namespace
	Resource     string     `json:"resource,omitempty"`      // Resource type (pods, services, etc.)
	Subresource  string     `json:"subresource,omitempty"`   // Resource subresource (status, scale, etc.)
	Name         string     `json:"name,omitempty"`          // Resource name
	APIGroup     string     `json:"api_group,omitempty"`     // API group
	APIVersion2  string     `json:"resource_version,omitempty"` // Resource API version
	
	// Request details
	RequestReceivedTime *time.Time `json:"request_received_time,omitempty"`
	StageTimestamp      *time.Time `json:"stage_timestamp,omitempty"`
	SourceIPs           []string   `json:"source_ips,omitempty"`
	UserAgent           string     `json:"user_agent,omitempty"`
	
	// Response details
	ResponseCode        *int32     `json:"response_code,omitempty"`
	ResponseStatus      string     `json:"response_status,omitempty"`
	ResponseReason      string     `json:"response_reason,omitempty"`
	
	// Object data (for create/update operations)
	ObjectResource      string     `json:"object_resource,omitempty"`
	ObjectNamespace     string     `json:"object_namespace,omitempty"`
	ObjectName          string     `json:"object_name,omitempty"`
	ObjectUID           string     `json:"object_uid,omitempty"`
	
	// Additional metadata
	ClusterName         string     `json:"cluster_name"`
	Annotations         map[string]interface{} `json:"annotations,omitempty"`
	
	// Tailpipe metadata (enriched automatically)
	SourceName          string     `json:"tp_source_name,omitempty"`
	SourceType          string     `json:"tp_source_type,omitempty"`
	CollectedAt         *time.Time `json:"tp_collected_at,omitempty"`

	// Required Tailpipe common fields for ingestion validation
	TpID                string     `json:"tp_id,omitempty"`
	TpTimestamp         *time.Time `json:"tp_timestamp,omitempty"`
	TpIngestTimestamp   *time.Time `json:"tp_ingest_timestamp,omitempty"`
	TpTable             string     `json:"tp_table,omitempty"`
	TpPartition         string     `json:"tp_partition,omitempty"`
	TpDate              *time.Time `json:"tp_date,omitempty"`
}

// AuditLogQueryFilters represents filters for audit log queries
type AuditLogQueryFilters struct {
	// Time range filters
	SinceTime  *time.Time
	UntilTime  *time.Time
	
	// User filters
	UserNames     []string
	UserGroups    []string
	
	// Resource filters
	Namespaces    []string
	Resources     []string
	Verbs         []string
	Levels        []string  // Metadata, Request, RequestResponse
	Stages        []string  // RequestReceived, ResponseStarted, ResponseComplete
	
	// Request filters
	SourceIPs     []string
	UserAgents    []string
	ResponseCodes []int32
	
	// Content filters
	URIPattern    *regexp.Regexp
	
	// Pagination
	Limit         *int64
	TailLines     *int64
}

// AuditLogQueryOptions represents user-friendly query options
type AuditLogQueryOptions struct {
	// Time range options
	SinceTime     *time.Time
	UntilTime     *time.Time
	SinceDuration string
	
	// User filtering
	UserNames     []string
	UserGroups    []string
	
	// Resource filtering
	Namespaces    []string
	Resources     []string
	Verbs         []string
	Levels        []string
	Stages        []string
	
	// Request filtering
	SourceIPs     []string
	UserAgents    []string
	ResponseCodes []int32
	
	// Content filtering
	URIPattern    string // regex pattern
	
	// Pagination and limits
	Limit         *int64
	TailLines     *int64
	
	// Collection options
	Follow        bool
}

// KubernetesAuditLogsTable implements the Table interface for audit logs
type KubernetesAuditLogsTable struct {
    client internal.KubernetesClientInterface
    config *config.Config

    // Audit log file paths
    auditLogPaths []string
	
	// File watching for real-time collection
	fileWatchers  map[string]context.CancelFunc
}

// Ensure the table implements the Table interface
var _ table.Table[*AuditLogRow] = (*KubernetesAuditLogsTable)(nil)

// NewKubernetesAuditLogsTable creates a new audit logs table
func NewKubernetesAuditLogsTable() *KubernetesAuditLogsTable {
    return &KubernetesAuditLogsTable{
        auditLogPaths: []string{
            "/var/log/audit/kube/kube-apiserver.log",           // Common path
            "/var/log/kube-audit/audit.log",                   // Alternative path
            "/var/log/kubernetes/audit.log",                   // Alternative path
            "/var/log/audit.log",                              // Generic path
        },
        fileWatchers: make(map[string]context.CancelFunc),
    }
}

// SetAuditLogPaths overrides the default file paths used when collecting from files.
func (t *KubernetesAuditLogsTable) SetAuditLogPaths(paths []string) {
    if len(paths) > 0 {
        t.auditLogPaths = paths
    }
}

// Factory function for table registration
func NewKubernetesAuditLogsTableFactory() table.Table[*AuditLogRow] {
	return NewKubernetesAuditLogsTable()
}

// Identifier returns the table name
func (t *KubernetesAuditLogsTable) Identifier() string {
	return "kubernetes_audit_logs"
}

// GetSourceMetadata returns source metadata for the table
func (t *KubernetesAuditLogsTable) GetSourceMetadata() ([]*table.SourceMetadata[*AuditLogRow], error) {
	return []*table.SourceMetadata[*AuditLogRow]{
		{
			SourceName: "kubernetes_audit_logs",
		},
	}, nil
}

// EnrichRow enriches an audit log row with common Tailpipe fields
func (t *KubernetesAuditLogsTable) EnrichRow(row *AuditLogRow, enrichment schema.SourceEnrichment) (*AuditLogRow, error) {
	if row == nil {
		return nil, fmt.Errorf("row cannot be nil")
	}
	
	// Set Tailpipe metadata from CommonFields
	if enrichment.CommonFields.TpSourceName != nil {
		row.SourceName = *enrichment.CommonFields.TpSourceName
	}
	row.SourceType = enrichment.CommonFields.TpSourceType

	// Ingest timestamp (now)
	now := time.Now().UTC()
	row.CollectedAt = &now
	row.TpIngestTimestamp = &now

	// Table/partition (from enrichment)
	row.TpTable = enrichment.CommonFields.TpTable
	row.TpPartition = enrichment.CommonFields.TpPartition

	// Event timestamp -> tp_timestamp
	var ts time.Time
	if row.StageTimestamp != nil {
		ts = row.StageTimestamp.UTC()
	} else if row.RequestReceivedTime != nil {
		ts = row.RequestReceivedTime.UTC()
	}
	if !ts.IsZero() {
		row.TpTimestamp = &ts
		// Derive tp_date as start of day UTC
		d := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		row.TpDate = &d
	}

	// ID - prefer audit id
	if row.TpID == "" {
		if row.AuditID != "" {
			row.TpID = row.AuditID
		} else {
			// fallback identity if audit id missing
			row.TpID = fmt.Sprintf("%s|%s|%s|%s|%s|%s", row.Verb, row.Resource, row.Namespace, row.Name, row.RequestURI, ts.UTC().Format(time.RFC3339Nano))
		}
	}

	return row, nil
}

// CollectRows is the main collection method called by the SDK
func (t *KubernetesAuditLogsTable) CollectRows(ctx context.Context) (<-chan *AuditLogRow, error) {
	// Create a channel for streaming rows
	rowChan := make(chan *AuditLogRow, 100)
	
	// Start collection in a goroutine
	go func() {
		defer close(rowChan)
		
		// Collect audit logs and stream to channel
		logs, err := t.collectAuditLogs(ctx)
		if err != nil {
			slog.Error("Failed to collect audit logs", "error", err)
			return
		}
		
		for _, log := range logs {
			select {
			case rowChan <- log:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return rowChan, nil
}

// collectAuditLogs collects audit logs from available sources
func (t *KubernetesAuditLogsTable) collectAuditLogs(ctx context.Context) ([]*AuditLogRow, error) {
	return t.collectAuditLogsWithFilters(ctx, AuditLogQueryFilters{})
}

// collectAuditLogsWithFilters collects audit logs with filtering support
func (t *KubernetesAuditLogsTable) collectAuditLogsWithFilters(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
	var allLogs []*AuditLogRow
	
	// Try different collection methods in order of preference
	
	// Method 1: Try to read from audit log files directly (if accessible)
	if logs, err := t.collectFromFiles(ctx, filters); err == nil && len(logs) > 0 {
		allLogs = append(allLogs, logs...)
		slog.Info("Collected audit logs from files", "count", len(logs))
	} else if err != nil {
		slog.Debug("Cannot access audit log files directly", "error", err)
	}
	
	// Method 2: Try to collect via API server pod logs (fallback)
	if len(allLogs) == 0 {
		if logs, err := t.collectFromAPIServerPods(ctx, filters); err == nil {
			allLogs = append(allLogs, logs...)
			slog.Info("Collected audit logs from API server pods", "count", len(logs))
		} else {
			slog.Warn("Failed to collect from API server pods", "error", err)
		}
	}
	
	// Method 3: Try to collect from DaemonSet/sidecar (if configured)
	if len(allLogs) == 0 {
		if logs, err := t.collectFromDaemonSet(ctx, filters); err == nil {
			allLogs = append(allLogs, logs...)
			slog.Info("Collected audit logs from DaemonSet", "count", len(logs))
		} else {
			slog.Debug("No DaemonSet audit collector found", "error", err)
		}
	}
	
	// Apply post-collection filters
	allLogs = t.applyPostCollectionFilters(allLogs, filters)
	
	if len(allLogs) == 0 {
		return nil, fmt.Errorf("no audit logs accessible - ensure audit logging is enabled and accessible via one of: file access, API server pod logs, or audit DaemonSet")
	}
	
	return allLogs, nil
}

// collectFromFiles attempts to read audit logs directly from files
func (t *KubernetesAuditLogsTable) collectFromFiles(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
	var allLogs []*AuditLogRow
	
	// Try each potential audit log path
	for _, path := range t.auditLogPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}
		
		slog.Debug("Found audit log file", "path", path)
		
		file, err := os.Open(path)
		if err != nil {
			slog.Debug("Cannot open audit log file", "path", path, "error", err)
			continue
		}
		defer file.Close()
		
		logs, err := t.parseAuditLogFile(file, filters)
		if err != nil {
			slog.Warn("Failed to parse audit log file", "path", path, "error", err)
			continue
		}
		
		allLogs = append(allLogs, logs...)
		
		// If we successfully read from one file, use it
		if len(logs) > 0 {
			slog.Info("Successfully read audit logs from file", "path", path, "count", len(logs))
			break
		}
	}
	
	if len(allLogs) == 0 {
		return nil, fmt.Errorf("no accessible audit log files found in paths: %v", t.auditLogPaths)
	}
	
	return allLogs, nil
}

// CollectFromFiles is an exported wrapper to collect audit logs from files.
func (t *KubernetesAuditLogsTable) CollectFromFiles(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
    return t.collectFromFiles(ctx, filters)
}

// collectFromAPIServerPods collects audit logs from API server pod logs
func (t *KubernetesAuditLogsTable) collectFromAPIServerPods(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
	if t.client == nil {
		return nil, fmt.Errorf("Kubernetes client not available")
	}
	
    // Find API server pods - try a few common label selectors used by different distros
    selectors := []string{
        "component=kube-apiserver",
        "k8s-app=kube-apiserver",
        "app=kube-apiserver",
        "tier=control-plane", // broad - will include other control plane pods, we'll parse logs for audit entries
    }
    var allPodObjs []runtime.Object
    for _, sel := range selectors {
        listOpts := internal.ListOptions{ Namespace: "kube-system", LabelSelector: sel }
        found, err := t.client.ListResources(ctx, "pods", listOpts)
        if err != nil {
            continue
        }
        allPodObjs = append(allPodObjs, found...)
    }
    if len(allPodObjs) == 0 {
        return nil, fmt.Errorf("no API server pods found (tried selectors: %v)", selectors)
    }
	
	var allLogs []*AuditLogRow
	
    // Collect logs from each API server pod
    for _, podObj := range allPodObjs {
		pod, ok := podObj.(*corev1.Pod)
		if !ok {
			continue
		}
		
        // Look for audit logs in pod logs (this is a fallback method)
        // IMPORTANT: Do NOT request k8s-added timestamps here. When enabled,
        // the log line is prefixed with a timestamp, which breaks JSON parsing
        // of the audit event line. We want raw JSON lines from stdout.
        logOpts := internal.LogOptions{
            Follow:     false,
            Timestamps: false,
        }
		
		if filters.SinceTime != nil {
			logOpts.SinceTime = &metav1.Time{Time: *filters.SinceTime}
		}
		
		logStream, err := t.client.GetPodLogs(ctx, pod.Namespace, pod.Name, "", logOpts)
		if err != nil {
			slog.Warn("Failed to get logs from API server pod", "pod", pod.Name, "error", err)
			continue
		}
		defer logStream.Close()
		
		// Parse logs looking for audit entries
		logs, err := t.parseAuditLogStream(logStream, filters)
		if err != nil {
			slog.Warn("Failed to parse API server pod logs", "pod", pod.Name, "error", err)
			continue
		}
		
		allLogs = append(allLogs, logs...)
	}
	
	return allLogs, nil
}

// CollectFromAPIServerPods is an exported wrapper to collect audit logs from API server pods.
func (t *KubernetesAuditLogsTable) CollectFromAPIServerPods(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
    return t.collectFromAPIServerPods(ctx, filters)
}

// collectFromDaemonSet collects audit logs from a DaemonSet audit collector
func (t *KubernetesAuditLogsTable) collectFromDaemonSet(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
	if t.client == nil {
		return nil, fmt.Errorf("Kubernetes client not available")
	}
	
	// Look for audit collector pods (common labels)
	auditLabels := []string{
		"app=audit-collector",
		"component=audit",
		"app.kubernetes.io/name=audit-logs",
	}
	
	for _, labelSelector := range auditLabels {
		listOpts := internal.ListOptions{
			LabelSelector: labelSelector,
		}
		
		pods, err := t.client.ListResources(ctx, "pods", listOpts)
		if err != nil {
			continue
		}
		
		if len(pods) > 0 {
			slog.Info("Found audit collector pods", "count", len(pods), "selector", labelSelector)
			// Implement collection from these pods
			// This would follow similar pattern to collectFromAPIServerPods
			// but looking for audit-specific containers/logs
		}
	}
	
	return nil, fmt.Errorf("no audit collector DaemonSet found")
}

// CollectFromDaemonSet is an exported wrapper to collect audit logs from a DaemonSet collector.
func (t *KubernetesAuditLogsTable) CollectFromDaemonSet(ctx context.Context, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
    return t.collectFromDaemonSet(ctx, filters)
}

// parseAuditLogFile parses an audit log file
func (t *KubernetesAuditLogsTable) parseAuditLogFile(file io.Reader, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
	var logs []*AuditLogRow
	
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024) // 2MB max line size for large audit entries
	
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		auditRow, err := t.parseAuditLogLine(line)
		if err != nil {
			slog.Debug("Failed to parse audit log line", "error", err, "line", line[:min(100, len(line))])
			continue
		}
		
		if auditRow != nil && t.passesFilters(auditRow, filters) {
			logs = append(logs, auditRow)
			lineCount++
			
			// Apply limit if specified
			if filters.Limit != nil && int64(lineCount) >= *filters.Limit {
				break
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading audit log file: %w", err)
	}
	
	return logs, nil
}

// parseAuditLogStream parses audit logs from a stream
func (t *KubernetesAuditLogsTable) parseAuditLogStream(stream io.Reader, filters AuditLogQueryFilters) ([]*AuditLogRow, error) {
	// Similar to parseAuditLogFile but for streams
	return t.parseAuditLogFile(stream, filters)
}

// parseAuditLogLine parses a single audit log line (JSON format)
func (t *KubernetesAuditLogsTable) parseAuditLogLine(line string) (*AuditLogRow, error) {
	// Kubernetes audit logs are in JSON format
	var auditEvent map[string]interface{}
	if err := json.Unmarshal([]byte(line), &auditEvent); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	
	// Check if this is actually an audit event
	if kind, ok := auditEvent["kind"].(string); !ok || kind != "Event" {
		return nil, fmt.Errorf("not an audit event")
	}
	
	row := &AuditLogRow{
		ClusterName: t.getClusterName(),
	}
	
	// Parse core fields
	if kind, ok := auditEvent["kind"].(string); ok {
		row.Kind = kind
	}
	if apiVersion, ok := auditEvent["apiVersion"].(string); ok {
		row.APIVersion = apiVersion
	}
	if level, ok := auditEvent["level"].(string); ok {
		row.Level = level
	}
	if auditID, ok := auditEvent["auditID"].(string); ok {
		row.AuditID = auditID
	}
	if stage, ok := auditEvent["stage"].(string); ok {
		row.Stage = stage
	}
	if requestURI, ok := auditEvent["requestURI"].(string); ok {
		row.RequestURI = requestURI
	}
	if verb, ok := auditEvent["verb"].(string); ok {
		row.Verb = verb
	}
	
	// Parse user information
	if user, ok := auditEvent["user"].(map[string]interface{}); ok {
		if username, ok := user["username"].(string); ok {
			row.UserName = username
		}
		if uid, ok := user["uid"].(string); ok {
			row.UserUID = uid
		}
		if groups, ok := user["groups"].([]interface{}); ok {
			for _, group := range groups {
				if groupStr, ok := group.(string); ok {
					row.UserGroups = append(row.UserGroups, groupStr)
				}
			}
		}
	}
	
	// Parse object information
	if objectRef, ok := auditEvent["objectRef"].(map[string]interface{}); ok {
		if namespace, ok := objectRef["namespace"].(string); ok {
			row.Namespace = namespace
		}
		if resource, ok := objectRef["resource"].(string); ok {
			row.Resource = resource
		}
		if subresource, ok := objectRef["subresource"].(string); ok {
			row.Subresource = subresource
		}
		if name, ok := objectRef["name"].(string); ok {
			row.Name = name
		}
		if apiGroup, ok := objectRef["apiGroup"].(string); ok {
			row.APIGroup = apiGroup
		}
		if apiVersion, ok := objectRef["apiVersion"].(string); ok {
			row.APIVersion2 = apiVersion
		}
	}
	
	// Parse timestamps
	if requestReceivedTimestamp, ok := auditEvent["requestReceivedTimestamp"].(string); ok {
		if ts, err := time.Parse(time.RFC3339Nano, requestReceivedTimestamp); err == nil {
			row.RequestReceivedTime = &ts
		}
	}
	if stageTimestamp, ok := auditEvent["stageTimestamp"].(string); ok {
		if ts, err := time.Parse(time.RFC3339Nano, stageTimestamp); err == nil {
			row.StageTimestamp = &ts
		}
	}
	
	// Parse source IPs
	if sourceIPs, ok := auditEvent["sourceIPs"].([]interface{}); ok {
		for _, ip := range sourceIPs {
			if ipStr, ok := ip.(string); ok {
				row.SourceIPs = append(row.SourceIPs, ipStr)
			}
		}
	}
	
	// Parse user agent
	if userAgent, ok := auditEvent["userAgent"].(string); ok {
		row.UserAgent = userAgent
	}
	
	// Parse response status
	if responseStatus, ok := auditEvent["responseStatus"].(map[string]interface{}); ok {
		if code, ok := responseStatus["code"].(float64); ok {
			responseCode := int32(code)
			row.ResponseCode = &responseCode
		}
		if status, ok := responseStatus["status"].(string); ok {
			row.ResponseStatus = status
		}
		if reason, ok := responseStatus["reason"].(string); ok {
			row.ResponseReason = reason
		}
	}
	
	// Parse annotations
	if annotations, ok := auditEvent["annotations"].(map[string]interface{}); ok {
		row.Annotations = annotations
	}
	
	return row, nil
}

// Helper methods for filtering and utilities

// passesFilters checks if an audit log row passes the specified filters
func (t *KubernetesAuditLogsTable) passesFilters(row *AuditLogRow, filters AuditLogQueryFilters) bool {
	// Time range filtering
	if filters.SinceTime != nil || filters.UntilTime != nil {
		var eventTime *time.Time
		if row.StageTimestamp != nil {
			eventTime = row.StageTimestamp
		} else if row.RequestReceivedTime != nil {
			eventTime = row.RequestReceivedTime
		}
		
		if eventTime != nil {
			if filters.SinceTime != nil && eventTime.Before(*filters.SinceTime) {
				return false
			}
			if filters.UntilTime != nil && eventTime.After(*filters.UntilTime) {
				return false
			}
		}
	}
	
	// User name filtering
	if len(filters.UserNames) > 0 && !t.containsString(filters.UserNames, row.UserName) {
		return false
	}
	
	// Namespace filtering
	if len(filters.Namespaces) > 0 && !t.matchesNamespaceFilter(row.Namespace, filters.Namespaces) {
		return false
	}
	
	// Resource filtering
	if len(filters.Resources) > 0 && !t.containsString(filters.Resources, row.Resource) {
		return false
	}
	
	// Verb filtering
	if len(filters.Verbs) > 0 && !t.containsString(filters.Verbs, row.Verb) {
		return false
	}
	
	// Level filtering
	if len(filters.Levels) > 0 && !t.containsString(filters.Levels, row.Level) {
		return false
	}
	
	// Stage filtering
	if len(filters.Stages) > 0 && !t.containsString(filters.Stages, row.Stage) {
		return false
	}
	
	// URI pattern filtering
	if filters.URIPattern != nil && !filters.URIPattern.MatchString(row.RequestURI) {
		return false
	}
	
	return true
}

// applyPostCollectionFilters applies final filters and limits
func (t *KubernetesAuditLogsTable) applyPostCollectionFilters(logs []*AuditLogRow, filters AuditLogQueryFilters) []*AuditLogRow {
	// Apply final limit
	if filters.Limit != nil && int64(len(logs)) > *filters.Limit {
		logs = logs[:*filters.Limit]
	}
	
	return logs
}

// Utility methods

// getClusterName returns the cluster name from config
func (t *KubernetesAuditLogsTable) getClusterName() string {
	if t.config != nil {
		return t.config.GetClusterName()
	}
	return ""
}

// matchesNamespaceFilter checks if a namespace matches the filter
func (t *KubernetesAuditLogsTable) matchesNamespaceFilter(namespace string, allowedNamespaces []string) bool {
	for _, allowed := range allowedNamespaces {
		if allowed == "*" || allowed == namespace {
			return true
		}
	}
	return false
}

// containsString checks if a slice contains a string
func (t *KubernetesAuditLogsTable) containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// SetClient sets the Kubernetes client and config
func (t *KubernetesAuditLogsTable) SetClient(client internal.KubernetesClientInterface, config *config.Config) {
	t.client = client
	t.config = config
}

// initializeClient initializes with default configuration (fallback)
func (t *KubernetesAuditLogsTable) initializeClient() error {
	// Create default configuration
	cfg := &config.Config{}
	cfg.ApplyDefaults()
	
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("default configuration validation failed: %w", err)
	}
	
	// For audit logs, we primarily need file access or K8s API access
	return fmt.Errorf("Kubernetes client not initialized. For audit logs, ensure plugin has access to audit log files or API server pods")
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Close cleans up resources
func (t *KubernetesAuditLogsTable) Close() error {
	// Cancel any active file watchers
	for path, cancelFunc := range t.fileWatchers {
		cancelFunc()
		delete(t.fileWatchers, path)
	}
	
	if t.client != nil {
		return t.client.Close()
	}
	return nil
}

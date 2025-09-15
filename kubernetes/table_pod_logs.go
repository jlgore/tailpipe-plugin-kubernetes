package kubernetes

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jlgore/tailpipe-plugin-kubernetes/config"
	"github.com/jlgore/tailpipe-plugin-kubernetes/internal"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// PodLogRow represents a single pod log entry
type PodLogRow struct {
	// Core log fields
	Timestamp     *time.Time `json:"timestamp"`
	Namespace     string     `json:"namespace"`
	PodName       string     `json:"pod_name"`
	ContainerName string     `json:"container_name"`
	LogMessage    string     `json:"log_message"`
	LogLevel      string     `json:"log_level"`

	// Pod metadata
	NodeName     string                 `json:"node_name"`
	Labels       map[string]interface{} `json:"labels"`
	Annotations  map[string]interface{} `json:"annotations"`
	RestartCount int                    `json:"restart_count"`

	// Cluster metadata
	ClusterName string `json:"cluster_name"`

	// Tailpipe metadata (enriched automatically)
	SourceName  string     `json:"tp_source_name,omitempty"`
	SourceType  string     `json:"tp_source_type,omitempty"`
	CollectedAt *time.Time `json:"tp_collected_at,omitempty"`
}

// LogQueryFilters represents filters for pod log queries
type LogQueryFilters struct {
	// Time range filters
	SinceTime *time.Time
	UntilTime *time.Time

	// Resource filters
	Namespaces     []string
	PodNamePattern *regexp.Regexp
	LabelSelector  labels.Selector

	// Log content filters
	LogLevels []string

	// Pagination
	Limit     *int64
	TailLines *int64
}

// KubernetesPodLogsTable implements the Table interface for pod logs
type KubernetesPodLogsTable struct {
	client internal.KubernetesClientInterface
	config *config.Config

	// Cache for streaming connections
	streamingConnections map[string]context.CancelFunc
}

// Ensure the table implements the Table interface
var _ table.Table[*PodLogRow] = (*KubernetesPodLogsTable)(nil)

// NewKubernetesPodLogsTable creates a new pod logs table
func NewKubernetesPodLogsTable() *KubernetesPodLogsTable {
	return &KubernetesPodLogsTable{
		streamingConnections: make(map[string]context.CancelFunc),
	}
}

// Factory function for table registration
func NewKubernetesPodLogsTableFactory() table.Table[*PodLogRow] {
	return NewKubernetesPodLogsTable()
}

// Identifier returns the table name
func (t *KubernetesPodLogsTable) Identifier() string {
	return "kubernetes_pod_logs"
}

// GetSourceMetadata returns source metadata for the table
func (t *KubernetesPodLogsTable) GetSourceMetadata() ([]*table.SourceMetadata[*PodLogRow], error) {
	return []*table.SourceMetadata[*PodLogRow]{
		{
			SourceName: "kubernetes_pod_logs",
		},
	}, nil
}

// EnrichRow enriches a pod log row with common Tailpipe fields
func (t *KubernetesPodLogsTable) EnrichRow(row *PodLogRow, enrichment schema.SourceEnrichment) (*PodLogRow, error) {
	if row == nil {
		return nil, fmt.Errorf("row cannot be nil")
	}

	// Set Tailpipe metadata from CommonFields
	if enrichment.CommonFields.TpSourceName != nil {
		row.SourceName = *enrichment.CommonFields.TpSourceName
	}
	row.SourceType = enrichment.CommonFields.TpSourceType

	now := time.Now()
	row.CollectedAt = &now

	return row, nil
}

// CollectRows is the main collection method called by the SDK
func (t *KubernetesPodLogsTable) CollectRows(ctx context.Context) (<-chan *PodLogRow, error) {
	if t.client == nil {
		if err := t.initializeClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
		}
	}

	// Create a channel for streaming rows
	rowChan := make(chan *PodLogRow, 100)

	// Start collection in a goroutine
	go func() {
		defer close(rowChan)

		// Collect pod logs and stream to channel
		logs, err := t.collectPodLogs(ctx)
		if err != nil {
			slog.Error("Failed to collect pod logs", "error", err)
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

// QueryPodLogsWithOptions provides an interface for querying pod logs with options
func (t *KubernetesPodLogsTable) QueryPodLogsWithOptions(ctx context.Context, options LogQueryOptions) ([]*PodLogRow, error) {
	filters := t.convertOptionsToFilters(options)
	return t.collectPodLogsWithFilters(ctx, filters)
}

// LogQueryOptions represents user-friendly query options for pod logs
type LogQueryOptions struct {
	// Time range options
	SinceTime     *time.Time
	UntilTime     *time.Time
	SinceDuration string // e.g., "1h", "30m"

	// Resource filtering
	Namespaces     []string
	PodNamePattern string   // regex pattern
	LabelSelectors []string // Kubernetes label selectors

	// Log content filtering
	LogLevels []string

	// Pagination and limits
	Limit     *int64
	TailLines *int64

	// Streaming options
	Follow bool
}

// convertOptionsToFilters converts LogQueryOptions to LogQueryFilters
func (t *KubernetesPodLogsTable) convertOptionsToFilters(options LogQueryOptions) LogQueryFilters {
	filters := LogQueryFilters{
		SinceTime:  options.SinceTime,
		UntilTime:  options.UntilTime,
		Namespaces: options.Namespaces,
		LogLevels:  options.LogLevels,
		Limit:      options.Limit,
		TailLines:  options.TailLines,
	}

	// Parse since duration if provided
	if options.SinceDuration != "" && options.SinceTime == nil {
		if duration, err := time.ParseDuration(options.SinceDuration); err == nil {
			since := time.Now().Add(-duration)
			filters.SinceTime = &since
		}
	}

	// Parse pod name pattern
	if options.PodNamePattern != "" {
		if regex, err := regexp.Compile(options.PodNamePattern); err == nil {
			filters.PodNamePattern = regex
		}
	}

	// Parse label selectors
	if len(options.LabelSelectors) > 0 {
		// Combine multiple label selectors with AND
		selectorStr := strings.Join(options.LabelSelectors, ",")
		if selector, err := labels.Parse(selectorStr); err == nil {
			filters.LabelSelector = selector
		}
	}

	return filters
}

// collectPodLogs collects logs from Kubernetes pods with optional filtering
func (t *KubernetesPodLogsTable) collectPodLogs(ctx context.Context) ([]*PodLogRow, error) {
	return t.collectPodLogsWithFilters(ctx, LogQueryFilters{})
}

// collectPodLogsWithFilters collects logs from Kubernetes pods with filtering support
func (t *KubernetesPodLogsTable) collectPodLogsWithFilters(ctx context.Context, filters LogQueryFilters) ([]*PodLogRow, error) {
	if t.client == nil {
		if err := t.initializeClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
		}
	}

	var allLogs []*PodLogRow

	// Build list options from filters
	listOpts := t.buildListOptions(filters)

	// Get pods that have logs available
	pods, err := t.client.GetPodsWithLogs(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods with logs: %w", err)
	}

	// Filter pods by name pattern if specified
	if filters.PodNamePattern != nil {
		var filteredPods []corev1.Pod
		for _, pod := range pods {
			if filters.PodNamePattern.MatchString(pod.Name) {
				filteredPods = append(filteredPods, pod)
			}
		}
		pods = filteredPods
	}

	slog.Info("Found pods with logs", "count", len(pods), "filters", logFiltersString(filters))

	// Collect logs from each pod
	for _, pod := range pods {
		podLogs, err := t.collectLogsFromPodWithFilters(ctx, &pod, filters)
		if err != nil {
			slog.Warn("Failed to collect logs from pod",
				"namespace", pod.Namespace,
				"pod", pod.Name,
				"error", err)
			continue
		}
		allLogs = append(allLogs, podLogs...)
	}

	// Apply post-collection filters
	allLogs = t.applyPostCollectionFilters(allLogs, filters)

	return allLogs, nil
}

// CollectWithFilters is an exported wrapper to collect pod logs with filters.
func (t *KubernetesPodLogsTable) CollectWithFilters(ctx context.Context, filters LogQueryFilters) ([]*PodLogRow, error) {
	return t.collectPodLogsWithFilters(ctx, filters)
}

// buildListOptions builds Kubernetes list options from query filters
func (t *KubernetesPodLogsTable) buildListOptions(filters LogQueryFilters) internal.ListOptions {
	opts := internal.ListOptions{}

	// Set namespace filter
	if len(filters.Namespaces) > 0 {
		if len(filters.Namespaces) == 1 && filters.Namespaces[0] != "*" {
			opts.Namespace = filters.Namespaces[0]
		}
		// For multiple namespaces, we'll need to query each separately
	}

	// Set label selector
	if filters.LabelSelector != nil {
		opts.LabelSelector = filters.LabelSelector.String()
	}

	return opts
}

// collectLogsFromPod collects logs from a specific pod
func (t *KubernetesPodLogsTable) collectLogsFromPod(ctx context.Context, pod *corev1.Pod) ([]*PodLogRow, error) {
	return t.collectLogsFromPodWithFilters(ctx, pod, LogQueryFilters{})
}

// collectLogsFromPodWithFilters collects logs from a specific pod with filtering
func (t *KubernetesPodLogsTable) collectLogsFromPodWithFilters(ctx context.Context, pod *corev1.Pod, filters LogQueryFilters) ([]*PodLogRow, error) {
	var logs []*PodLogRow

	// Collect logs from each container in the pod
	for _, container := range pod.Spec.Containers {
		containerLogs, err := t.collectLogsFromContainerWithFilters(ctx, pod, container.Name, filters)
		if err != nil {
			// Enhanced error handling for permissions and access issues
			if t.isPermissionError(err) {
				slog.Warn("Permission denied accessing container logs",
					"namespace", pod.Namespace,
					"pod", pod.Name,
					"container", container.Name,
					"error", err)
				continue
			}
			slog.Warn("Failed to collect logs from container",
				"namespace", pod.Namespace,
				"pod", pod.Name,
				"container", container.Name,
				"error", err)
			continue
		}
		logs = append(logs, containerLogs...)
	}

	// Also collect from init containers
	for _, container := range pod.Spec.InitContainers {
		containerLogs, err := t.collectLogsFromContainerWithFilters(ctx, pod, container.Name, filters)
		if err != nil {
			if t.isPermissionError(err) {
				slog.Warn("Permission denied accessing init container logs",
					"namespace", pod.Namespace,
					"pod", pod.Name,
					"container", container.Name,
					"error", err)
				continue
			}
			slog.Warn("Failed to collect logs from init container",
				"namespace", pod.Namespace,
				"pod", pod.Name,
				"container", container.Name,
				"error", err)
			continue
		}
		logs = append(logs, containerLogs...)
	}

	return logs, nil
}

// collectLogsFromContainer collects logs from a specific container
func (t *KubernetesPodLogsTable) collectLogsFromContainer(ctx context.Context, pod *corev1.Pod, containerName string) ([]*PodLogRow, error) {
	return t.collectLogsFromContainerWithFilters(ctx, pod, containerName, LogQueryFilters{})
}

// collectLogsFromContainerWithFilters collects logs from a specific container with filtering
func (t *KubernetesPodLogsTable) collectLogsFromContainerWithFilters(ctx context.Context, pod *corev1.Pod, containerName string, filters LogQueryFilters) ([]*PodLogRow, error) {
	// Configure log options based on plugin configuration and filters
	logOpts := internal.LogOptions{
		Follow:     t.config.GetFollowLogs(),
		Timestamps: true,
	}

	// Apply time range filters
	if filters.SinceTime != nil {
		logOpts.SinceTime = &metav1.Time{Time: *filters.SinceTime}
	} else if sinceTime := t.config.GetSinceTime(); sinceTime != "" {
		if duration, err := time.ParseDuration(sinceTime); err == nil {
			since := metav1.NewTime(time.Now().Add(-duration))
			logOpts.SinceTime = &since
		}
	}

	// Apply pagination filters
	if filters.TailLines != nil {
		logOpts.TailLines = filters.TailLines
	}
	if filters.Limit != nil {
		logOpts.LimitBytes = filters.Limit
	}

	// Get log stream with enhanced error handling
	logStream, err := t.client.GetPodLogs(ctx, pod.Namespace, pod.Name, containerName, logOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get log stream for %s/%s[%s]: %w", pod.Namespace, pod.Name, containerName, err)
	}
	defer logStream.Close()

	// Read and parse logs with advanced parsing
	logs, err := t.parseLogStreamWithFilters(logStream, pod, containerName, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log stream: %w", err)
	}

	return logs, nil
}

// parseLogStream parses the log stream and converts to PodLogRow entries
func (t *KubernetesPodLogsTable) parseLogStream(stream io.ReadCloser, pod *corev1.Pod, containerName string) ([]*PodLogRow, error) {
	return t.parseLogStreamWithFilters(stream, pod, containerName, LogQueryFilters{})
}

// parseLogStreamWithFilters parses the log stream with advanced parsing and filtering
func (t *KubernetesPodLogsTable) parseLogStreamWithFilters(stream io.ReadCloser, pod *corev1.Pod, containerName string, filters LogQueryFilters) ([]*PodLogRow, error) {
	var logs []*PodLogRow

	// Use buffered scanner for better performance with streaming logs
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB max line size

	// Get container restart count
	restartCount := 0
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == containerName {
			restartCount = int(containerStatus.RestartCount)
			break
		}
	}

	// Convert labels and annotations to map[string]interface{}
	labels := make(map[string]interface{})
	for k, v := range pod.Labels {
		labels[k] = v
	}

	annotations := make(map[string]interface{})
	for k, v := range pod.Annotations {
		annotations[k] = v
	}

	// Process each log line with streaming support
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		logRow := &PodLogRow{
			Namespace:     pod.Namespace,
			PodName:       pod.Name,
			ContainerName: containerName,
			LogMessage:    line,
			NodeName:      pod.Spec.NodeName,
			Labels:        labels,
			Annotations:   annotations,
			RestartCount:  restartCount,
			ClusterName:   t.config.GetClusterName(),
		}

		// Parse timestamp and log level with enhanced parsing
		t.parseLogLineAdvanced(logRow, line)

		// Apply inline log level filtering
		if len(filters.LogLevels) > 0 && !t.matchesLogLevelFilter(logRow.LogLevel, filters.LogLevels) {
			continue
		}

		// Apply time range filtering if timestamp was parsed
		if logRow.Timestamp != nil {
			if filters.SinceTime != nil && logRow.Timestamp.Before(*filters.SinceTime) {
				continue
			}
			if filters.UntilTime != nil && logRow.Timestamp.After(*filters.UntilTime) {
				continue
			}
		}

		logs = append(logs, logRow)
		lineCount++

		// Apply limit if specified
		if filters.Limit != nil && int64(lineCount) >= *filters.Limit {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading log stream: %w", err)
	}

	return logs, nil
}

// parseLogLine extracts timestamp and log level from a log line (legacy method)
func (t *KubernetesPodLogsTable) parseLogLine(logRow *PodLogRow, line string) {
	t.parseLogLineAdvanced(logRow, line)
}

// parseLogLineAdvanced extracts timestamp and log level with enhanced parsing
func (t *KubernetesPodLogsTable) parseLogLineAdvanced(logRow *PodLogRow, line string) {
	// Try to parse RFC3339 timestamp at the beginning of the line
	// Kubernetes logs often start with a timestamp like "2023-12-01T10:30:45.123456789Z"
	if len(line) > 30 {
		timestampStr := line[:30]
		if timestamp, err := time.Parse(time.RFC3339Nano, timestampStr); err == nil {
			logRow.Timestamp = &timestamp
			// Remove timestamp from log message
			if len(line) > 31 {
				logRow.LogMessage = strings.TrimSpace(line[31:])
			}
		}
	}

	// Try to extract log level from the message with advanced parsing
	logRow.LogLevel = t.extractLogLevelAdvanced(logRow.LogMessage)
}

// extractLogLevel attempts to extract log level from log message (legacy method)
func (t *KubernetesPodLogsTable) extractLogLevel(message string) string {
	return t.extractLogLevelAdvanced(message)
}

// extractLogLevelAdvanced attempts to extract log level with enhanced parsing for JSON and logfmt
func (t *KubernetesPodLogsTable) extractLogLevelAdvanced(message string) string {
	// Try JSON parsing first
	if level := t.extractLogLevelFromJSON(message); level != "" {
		return level
	}

	// Try logfmt parsing
	if level := t.extractLogLevelFromLogfmt(message); level != "" {
		return level
	}

	// Fallback to pattern matching
	return t.extractLogLevelFromPattern(message)
}

// extractLogLevelFromJSON extracts log level from JSON formatted logs
func (t *KubernetesPodLogsTable) extractLogLevelFromJSON(message string) string {
	// Check if the message looks like JSON
	message = strings.TrimSpace(message)
	if !strings.HasPrefix(message, "{") {
		return ""
	}

	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(message), &logEntry); err != nil {
		return ""
	}

	// Common JSON log level fields
	levelFields := []string{"level", "lvl", "severity", "log_level", "loglevel"}
	for _, field := range levelFields {
		if val, exists := logEntry[field]; exists {
			if level, ok := val.(string); ok {
				return strings.ToUpper(level)
			}
		}
	}

	return ""
}

// extractLogLevelFromLogfmt extracts log level from logfmt formatted logs
func (t *KubernetesPodLogsTable) extractLogLevelFromLogfmt(message string) string {
	// Look for level=value pattern
	levelRegex := regexp.MustCompile(`\b(?:level|lvl|severity)\s*=\s*([^\s]+)`)
	matches := levelRegex.FindStringSubmatch(message)
	if len(matches) > 1 {
		return strings.ToUpper(strings.Trim(matches[1], `"'`))
	}
	return ""
}

// extractLogLevelFromPattern extracts log level using pattern matching
func (t *KubernetesPodLogsTable) extractLogLevelFromPattern(message string) string {
	message = strings.ToUpper(message)

	// Enhanced log level patterns with priority order
	logLevelPatterns := []struct {
		pattern string
		level   string
	}{
		{"FATAL", "FATAL"},
		{"ERROR", "ERROR"},
		{"WARN", "WARN"},
		{"WARNING", "WARN"},
		{"INFO", "INFO"},
		{"DEBUG", "DEBUG"},
		{"TRACE", "TRACE"},
	}

	for _, levelPattern := range logLevelPatterns {
		if strings.Contains(message, levelPattern.pattern) {
			return levelPattern.level
		}
	}

	return "INFO" // Default level
}

// SetClient sets the Kubernetes client and config
func (t *KubernetesPodLogsTable) SetClient(client internal.KubernetesClientInterface, config *config.Config) {
	t.client = client
	t.config = config
}

// initializeClient initializes with default configuration (fallback)
func (t *KubernetesPodLogsTable) initializeClient() error {
	// Create default configuration
	cfg := &config.Config{}
	cfg.ApplyDefaults()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("default configuration validation failed: %w", err)
	}

	// For now, return an error that guides users to use proper configuration
	// In a production plugin, this would create a default client with the config
	return fmt.Errorf("Kubernetes client not initialized. Please ensure plugin is configured with valid kubeconfig or cluster credentials")
}

// Helper methods for filtering and utilities

// applyPostCollectionFilters applies filters that couldn't be applied during collection
func (t *KubernetesPodLogsTable) applyPostCollectionFilters(logs []*PodLogRow, filters LogQueryFilters) []*PodLogRow {
	var filtered []*PodLogRow

	for _, log := range logs {
		// Apply namespace filtering (if multiple namespaces were queried)
		if len(filters.Namespaces) > 0 && !t.matchesNamespaceFilter(log.Namespace, filters.Namespaces) {
			continue
		}

		filtered = append(filtered, log)
	}

	// Apply final limit if not applied during streaming
	if filters.Limit != nil && int64(len(filtered)) > *filters.Limit {
		filtered = filtered[:*filters.Limit]
	}

	return filtered
}

// matchesLogLevelFilter checks if a log level matches the filter
func (t *KubernetesPodLogsTable) matchesLogLevelFilter(logLevel string, allowedLevels []string) bool {
	logLevel = strings.ToUpper(logLevel)
	for _, allowed := range allowedLevels {
		if strings.ToUpper(allowed) == logLevel {
			return true
		}
	}
	return false
}

// matchesNamespaceFilter checks if a namespace matches the filter
func (t *KubernetesPodLogsTable) matchesNamespaceFilter(namespace string, allowedNamespaces []string) bool {
	for _, allowed := range allowedNamespaces {
		if allowed == "*" || allowed == namespace {
			return true
		}
	}
	return false
}

// isPermissionError checks if an error is related to permissions
func (t *KubernetesPodLogsTable) isPermissionError(err error) bool {
	errorStr := strings.ToLower(err.Error())
	return strings.Contains(errorStr, "forbidden") ||
		strings.Contains(errorStr, "unauthorized") ||
		strings.Contains(errorStr, "permission denied") ||
		strings.Contains(errorStr, "access denied")
}

// logFiltersString creates a string representation of filters for logging
func logFiltersString(filters LogQueryFilters) string {
	var parts []string

	if len(filters.Namespaces) > 0 {
		parts = append(parts, fmt.Sprintf("namespaces=%v", filters.Namespaces))
	}
	if filters.PodNamePattern != nil {
		parts = append(parts, fmt.Sprintf("pod_pattern=%s", filters.PodNamePattern.String()))
	}
	if len(filters.LogLevels) > 0 {
		parts = append(parts, fmt.Sprintf("log_levels=%v", filters.LogLevels))
	}
	if filters.SinceTime != nil {
		parts = append(parts, fmt.Sprintf("since=%s", filters.SinceTime.Format(time.RFC3339)))
	}
	if filters.UntilTime != nil {
		parts = append(parts, fmt.Sprintf("until=%s", filters.UntilTime.Format(time.RFC3339)))
	}

	return strings.Join(parts, ", ")
}

// Real-time streaming support methods

// startLogStreaming starts real-time log streaming for a pod
func (t *KubernetesPodLogsTable) startLogStreaming(ctx context.Context, pod *corev1.Pod, containerName string, filters LogQueryFilters) (<-chan *PodLogRow, error) {
	logChan := make(chan *PodLogRow, 100) // Buffered channel

	// Configure streaming log options
	logOpts := internal.LogOptions{
		Follow:     true,
		Timestamps: true,
	}

	if filters.SinceTime != nil {
		logOpts.SinceTime = &metav1.Time{Time: *filters.SinceTime}
	}
	if filters.TailLines != nil {
		logOpts.TailLines = filters.TailLines
	}

	go func() {
		defer close(logChan)

		logStream, err := t.client.GetPodLogs(ctx, pod.Namespace, pod.Name, containerName, logOpts)
		if err != nil {
			slog.Error("Failed to start log streaming", "error", err)
			return
		}
		defer logStream.Close()

		// Process streaming logs
		scanner := bufio.NewScanner(logStream)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				line := scanner.Text()
				if strings.TrimSpace(line) == "" {
					continue
				}

				// Create log row (similar to parseLogStreamWithFilters)
				logRow := t.createLogRowFromLine(line, pod, containerName)

				// Apply filters
				if t.passesStreamingFilters(logRow, filters) {
					select {
					case logChan <- logRow:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return logChan, nil
}

// createLogRowFromLine creates a PodLogRow from a log line
func (t *KubernetesPodLogsTable) createLogRowFromLine(line string, pod *corev1.Pod, containerName string) *PodLogRow {
	// Get container restart count
	restartCount := 0
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == containerName {
			restartCount = int(containerStatus.RestartCount)
			break
		}
	}

	// Convert labels and annotations
	labels := make(map[string]interface{})
	for k, v := range pod.Labels {
		labels[k] = v
	}

	annotations := make(map[string]interface{})
	for k, v := range pod.Annotations {
		annotations[k] = v
	}

	logRow := &PodLogRow{
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		ContainerName: containerName,
		LogMessage:    line,
		NodeName:      pod.Spec.NodeName,
		Labels:        labels,
		Annotations:   annotations,
		RestartCount:  restartCount,
		ClusterName:   t.config.GetClusterName(),
	}

	// Parse timestamp and log level
	t.parseLogLineAdvanced(logRow, line)

	return logRow
}

// passesStreamingFilters checks if a log row passes streaming filters
func (t *KubernetesPodLogsTable) passesStreamingFilters(logRow *PodLogRow, filters LogQueryFilters) bool {
	// Apply log level filtering
	if len(filters.LogLevels) > 0 && !t.matchesLogLevelFilter(logRow.LogLevel, filters.LogLevels) {
		return false
	}

	// Apply time range filtering
	if logRow.Timestamp != nil {
		if filters.SinceTime != nil && logRow.Timestamp.Before(*filters.SinceTime) {
			return false
		}
		if filters.UntilTime != nil && logRow.Timestamp.After(*filters.UntilTime) {
			return false
		}
	}

	return true
}

// Close cleans up resources
func (t *KubernetesPodLogsTable) Close() error {
	// Cancel any active streaming connections
	for key, cancelFunc := range t.streamingConnections {
		cancelFunc()
		delete(t.streamingConnections, key)
	}

	if t.client != nil {
		return t.client.Close()
	}
	return nil
}

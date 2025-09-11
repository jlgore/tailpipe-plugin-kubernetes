package kubernetes

import (
	"context"
	"crypto/md5"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-kubernetes/config"
	"github.com/turbot/tailpipe-plugin-kubernetes/internal"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// EventRow represents a single Kubernetes event
type EventRow struct {
	// Core event fields
	Timestamp       *time.Time `json:"timestamp"`
	Namespace       string     `json:"namespace"`
	EventType       string     `json:"event_type"`       // Normal, Warning
	Reason          string     `json:"reason"`
	Message         string     `json:"message"`
	
	// Object information
	ObjectKind      string `json:"object_kind"`      // Pod, Service, etc.
	ObjectName      string `json:"object_name"`
	ObjectUID       string `json:"object_uid,omitempty"`
	
	// Source information
	SourceComponent string `json:"source_component"`
	SourceHost      string `json:"source_host,omitempty"`
	
	// Event timing
	FirstTimestamp  *time.Time `json:"first_timestamp"`
	LastTimestamp   *time.Time `json:"last_timestamp"`
	Count           int        `json:"count"`
	
	// Cluster metadata
	ClusterName     string `json:"cluster_name"`
	
	// Tailpipe metadata (enriched automatically)
	SourceName      string     `json:"tp_source_name,omitempty"`
	SourceType      string     `json:"tp_source_type,omitempty"`
	CollectedAt     *time.Time `json:"tp_collected_at,omitempty"`
	
	// Internal fields for deduplication
	DeduplicationKey string `json:"-"`
}

// EventQueryFilters represents filters for event queries
type EventQueryFilters struct {
	// Time range filters
	SinceTime  *time.Time
	UntilTime  *time.Time
	
	// Resource filters
	Namespaces      []string
	EventTypes      []string      // Normal, Warning
	ObjectKinds     []string      // Pod, Service, etc.
	ObjectNames     []string
	SourceComponents []string
	LabelSelector   labels.Selector
	
	// Content filters
	MessagePattern  *regexp.Regexp
	ReasonPattern   *regexp.Regexp
	
	// Pagination
	Limit         *int64
	
	// Watch options
	Watch         bool
	WatchTimeout  *time.Duration
}

// EventQueryOptions represents user-friendly query options for events
type EventQueryOptions struct {
	// Time range options
	SinceTime     *time.Time
	UntilTime     *time.Time
	SinceDuration string // e.g., "1h", "30m"
	
	// Resource filtering
	Namespaces      []string
	EventTypes      []string // Normal, Warning
	ObjectKinds     []string // Pod, Service, etc.
	ObjectNames     []string
	SourceComponents []string
	LabelSelectors  []string
	
	// Content filtering
	MessagePattern  string // regex pattern
	ReasonPattern   string // regex pattern
	
	// Pagination and limits
	Limit         *int64
	
	// Watch options
	Watch         bool
	WatchTimeout  *time.Duration
}

// KubernetesEventsTable implements the Table interface for cluster events
type KubernetesEventsTable struct {
	client internal.KubernetesClientInterface
	config *config.Config
	
	// Event deduplication and caching
	eventCache     map[string]*EventRow
	cacheMutex     sync.RWMutex
	
	// Watch management
	watchers       map[string]context.CancelFunc
	watchersMutex  sync.RWMutex
	
	// Reconnection logic
	reconnectDelay time.Duration
	maxReconnects  int
}

// Ensure the table implements the Table interface
var _ table.Table[*EventRow] = (*KubernetesEventsTable)(nil)

// NewKubernetesEventsTable creates a new events table
func NewKubernetesEventsTable() *KubernetesEventsTable {
	return &KubernetesEventsTable{
		eventCache:     make(map[string]*EventRow),
		watchers:       make(map[string]context.CancelFunc),
		reconnectDelay: 5 * time.Second,
		maxReconnects:  10,
	}
}

// Factory function for table registration
func NewKubernetesEventsTableFactory() table.Table[*EventRow] {
	return NewKubernetesEventsTable()
}

// Identifier returns the table name
func (t *KubernetesEventsTable) Identifier() string {
	return "kubernetes_events"
}

// GetSourceMetadata returns source metadata for the table
func (t *KubernetesEventsTable) GetSourceMetadata() ([]*table.SourceMetadata[*EventRow], error) {
	return []*table.SourceMetadata[*EventRow]{
		{
			SourceName: "kubernetes_events",
		},
	}, nil
}

// EnrichRow enriches an event row with common Tailpipe fields
func (t *KubernetesEventsTable) EnrichRow(row *EventRow, enrichment schema.SourceEnrichment) (*EventRow, error) {
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
func (t *KubernetesEventsTable) CollectRows(ctx context.Context) (<-chan *EventRow, error) {
	if t.client == nil {
		if err := t.initializeClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
		}
	}
	
	// Create a channel for streaming rows
	rowChan := make(chan *EventRow, 100)
	
	// Start collection in a goroutine
	go func() {
		defer close(rowChan)
		
		// Collect events and stream to channel
		events, err := t.collectEvents(ctx)
		if err != nil {
			slog.Error("Failed to collect events", "error", err)
			return
		}
		
		for _, event := range events {
			select {
			case rowChan <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return rowChan, nil
}

// collectEvents collects events from Kubernetes cluster
func (t *KubernetesEventsTable) collectEvents(ctx context.Context) ([]*EventRow, error) {
	return t.collectEventsWithFilters(ctx, EventQueryFilters{})
}

// collectEventsWithFilters collects events from Kubernetes cluster with filtering
func (t *KubernetesEventsTable) collectEventsWithFilters(ctx context.Context, filters EventQueryFilters) ([]*EventRow, error) {
	if t.client == nil {
		if err := t.initializeClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
		}
	}
	
	// If watch is requested, start watching
	if filters.Watch {
		return t.watchEvents(ctx, filters)
	}
	
	// Build list options from filters
	listOpts := t.buildListOptions(filters)
	
	// Get events from cluster
	events, err := t.client.ListEvents(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to list events: %w", err)
	}
	
	slog.Info("Found events", "count", len(events), "filters", t.logFiltersString(filters))
	
	// Convert and filter Kubernetes events to EventRow
	var eventRows []*EventRow
	for _, event := range events {
		eventRow := t.convertEventToRowWithFilters(&event, filters)
		if eventRow != nil {
			// Apply deduplication
			if dedupedRow := t.deduplicateEvent(eventRow); dedupedRow != nil {
				eventRows = append(eventRows, dedupedRow)
			}
		}
	}
	
	// Apply post-collection filters
	eventRows = t.applyPostCollectionFilters(eventRows, filters)
	
	return eventRows, nil
}

// CollectWithFilters is an exported wrapper to collect events with filtering.
func (t *KubernetesEventsTable) CollectWithFilters(ctx context.Context, filters EventQueryFilters) ([]*EventRow, error) {
    return t.collectEventsWithFilters(ctx, filters)
}

// convertEventToRow converts a Kubernetes event to an EventRow
func (t *KubernetesEventsTable) convertEventToRow(event *corev1.Event) *EventRow {
	return t.convertEventToRowWithFilters(event, EventQueryFilters{})
}

// convertEventToRowWithFilters converts a Kubernetes event to an EventRow with filtering
func (t *KubernetesEventsTable) convertEventToRowWithFilters(event *corev1.Event, filters EventQueryFilters) *EventRow {
	// Early filtering based on basic criteria
	if !t.passesBasicFilters(event, filters) {
		return nil
	}
	
	row := &EventRow{
		Namespace:       event.Namespace,
		EventType:       event.Type,
		Reason:          event.Reason,
		Message:         event.Message,
		ObjectKind:      event.InvolvedObject.Kind,
		ObjectName:      event.InvolvedObject.Name,
		ObjectUID:       string(event.InvolvedObject.UID),
		SourceComponent: event.Source.Component,
		SourceHost:      event.Source.Host,
		Count:           int(event.Count),
		ClusterName:     t.config.GetClusterName(),
	}
	
	// Set timestamps
	now := time.Now()
	row.Timestamp = &now
	
	if !event.FirstTimestamp.IsZero() {
		row.FirstTimestamp = &event.FirstTimestamp.Time
	}
	
	if !event.LastTimestamp.IsZero() {
		row.LastTimestamp = &event.LastTimestamp.Time
	} else if !event.EventTime.IsZero() {
		// Use EventTime if LastTimestamp is not available
		row.LastTimestamp = &event.EventTime.Time
	}
	
	// Use the most recent timestamp as the main timestamp
	if row.LastTimestamp != nil {
		row.Timestamp = row.LastTimestamp
	} else if row.FirstTimestamp != nil {
		row.Timestamp = row.FirstTimestamp
	}
	
	// Generate deduplication key
	row.DeduplicationKey = t.generateDeduplicationKey(event)
	
	// Apply content filters
	if !t.passesContentFilters(row, filters) {
		return nil
	}
	
	return row
}

// SetClient sets the Kubernetes client and config
func (t *KubernetesEventsTable) SetClient(client internal.KubernetesClientInterface, config *config.Config) {
	t.client = client
	t.config = config
}

// initializeClient initializes with default configuration (fallback)
func (t *KubernetesEventsTable) initializeClient() error {
	// Create default configuration
	cfg := &config.Config{}
	cfg.ApplyDefaults()
	
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("default configuration validation failed: %w", err)
	}
	
	// For now, return an error that guides users to use proper configuration
	return fmt.Errorf("Kubernetes client not initialized. Please ensure plugin is configured with valid kubeconfig or cluster credentials")
}

// Core functionality for event watching, filtering, and deduplication

// watchEvents implements real-time event watching with reconnection logic
func (t *KubernetesEventsTable) watchEvents(ctx context.Context, filters EventQueryFilters) ([]*EventRow, error) {
	eventChan := make(chan *EventRow, 100)
	errorChan := make(chan error, 1)
	
	// Start watching in a goroutine
	go t.startEventWatcher(ctx, filters, eventChan, errorChan)
	
	// Collect events from the channel
	var events []*EventRow
	timeout := 30 * time.Second
	if filters.WatchTimeout != nil {
		timeout = *filters.WatchTimeout
	}
	
	select {
	case <-time.After(timeout):
		// Return collected events after timeout
		return events, nil
	case err := <-errorChan:
		return events, fmt.Errorf("watch error: %w", err)
	case <-ctx.Done():
		return events, ctx.Err()
	default:
		// Collect events as they come
		for {
			select {
			case event := <-eventChan:
				if event != nil {
					events = append(events, event)
					// Apply limit if specified
					if filters.Limit != nil && int64(len(events)) >= *filters.Limit {
						return events, nil
					}
				}
			case <-time.After(timeout):
				return events, nil
			case err := <-errorChan:
				return events, fmt.Errorf("watch error: %w", err)
			case <-ctx.Done():
				return events, ctx.Err()
			}
		}
	}
}

// startEventWatcher starts watching for events with reconnection logic
func (t *KubernetesEventsTable) startEventWatcher(ctx context.Context, filters EventQueryFilters, eventChan chan<- *EventRow, errorChan chan<- error) {
	reconnectCount := 0
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// Build list options for watching
		listOpts := t.buildListOptions(filters)
		
		// Start watching
		watcher, err := t.client.WatchEvents(ctx, listOpts)
		if err != nil {
			slog.Error("Failed to start event watcher", "error", err, "reconnect_count", reconnectCount)
			
			// Handle reconnection
			if reconnectCount >= t.maxReconnects {
				errorChan <- fmt.Errorf("max reconnection attempts reached: %w", err)
				return
			}
			
			reconnectCount++
			select {
			case <-time.After(t.reconnectDelay):
				continue
			case <-ctx.Done():
				return
			}
		}
		
		// Reset reconnect count on successful connection
		reconnectCount = 0
		
		// Process watch results
		t.processWatchResults(ctx, watcher, filters, eventChan, errorChan)
		
		// If we get here, the watcher stopped, so we need to reconnect
		slog.Info("Event watcher stopped, attempting to reconnect", "reconnect_count", reconnectCount)
	}
}

// processWatchResults processes results from the event watcher
func (t *KubernetesEventsTable) processWatchResults(ctx context.Context, watcher internal.EventWatcher, filters EventQueryFilters, eventChan chan<- *EventRow, errorChan chan<- error) {
	defer watcher.Stop()
	
	for {
		select {
		case result := <-watcher.ResultChan():
			if result.Error != nil {
				slog.Error("Event watch error", "error", result.Error)
				errorChan <- result.Error
				return
			}
			
			if result.Object != nil {
				// Convert event to EventRow
				eventRow := t.convertEventToRowWithFilters(result.Object, filters)
				if eventRow != nil {
					// Apply deduplication
					if dedupedRow := t.deduplicateEvent(eventRow); dedupedRow != nil {
						select {
						case eventChan <- dedupedRow:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Event deduplication logic

// generateDeduplicationKey generates a unique key for event deduplication
func (t *KubernetesEventsTable) generateDeduplicationKey(event *corev1.Event) string {
	// Create a unique key based on reason, object, and source
	key := fmt.Sprintf("%s:%s:%s:%s:%s", 
		event.Namespace,
		event.Reason,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Name,
		event.Source.Component)
	
	// Hash the key to keep it manageable
	hash := md5.Sum([]byte(key))
	return fmt.Sprintf("%x", hash)
}

// deduplicateEvent handles event deduplication based on reason and object
func (t *KubernetesEventsTable) deduplicateEvent(event *EventRow) *EventRow {
	t.cacheMutex.Lock()
	defer t.cacheMutex.Unlock()
	
	existingEvent, exists := t.eventCache[event.DeduplicationKey]
	if !exists {
		// New event, add to cache
		t.eventCache[event.DeduplicationKey] = event
		return event
	}
	
	// Event exists, update count and timestamp
	existingEvent.Count += event.Count
	if event.LastTimestamp != nil {
		existingEvent.LastTimestamp = event.LastTimestamp
		existingEvent.Timestamp = event.LastTimestamp
	}
	
	// Update the cache
	t.eventCache[event.DeduplicationKey] = existingEvent
	
	// Return the updated event
	return existingEvent
}

// Filtering logic

// buildListOptions builds Kubernetes list options from query filters
func (t *KubernetesEventsTable) buildListOptions(filters EventQueryFilters) internal.ListOptions {
	opts := internal.ListOptions{}
	
	// Set namespace filter
	if len(filters.Namespaces) > 0 {
		if len(filters.Namespaces) == 1 && filters.Namespaces[0] != "*" {
			opts.Namespace = filters.Namespaces[0]
		}
	}
	
	// Set label selector
	if filters.LabelSelector != nil {
		opts.LabelSelector = filters.LabelSelector.String()
	}
	
	return opts
}

// passesBasicFilters checks if an event passes basic filters
func (t *KubernetesEventsTable) passesBasicFilters(event *corev1.Event, filters EventQueryFilters) bool {
	// Time range filtering
	if filters.SinceTime != nil || filters.UntilTime != nil {
		var eventTime time.Time
		if !event.LastTimestamp.IsZero() {
			eventTime = event.LastTimestamp.Time
		} else if !event.FirstTimestamp.IsZero() {
			eventTime = event.FirstTimestamp.Time
		} else if !event.EventTime.IsZero() {
			eventTime = event.EventTime.Time
		} else {
			return false // No valid timestamp
		}
		
		if filters.SinceTime != nil && eventTime.Before(*filters.SinceTime) {
			return false
		}
		if filters.UntilTime != nil && eventTime.After(*filters.UntilTime) {
			return false
		}
	}
	
	// Namespace filtering (if multiple namespaces)
	if len(filters.Namespaces) > 0 && !t.matchesNamespaceFilter(event.Namespace, filters.Namespaces) {
		return false
	}
	
	// Event type filtering
	if len(filters.EventTypes) > 0 && !t.containsString(filters.EventTypes, event.Type) {
		return false
	}
	
	// Object kind filtering
	if len(filters.ObjectKinds) > 0 && !t.containsString(filters.ObjectKinds, event.InvolvedObject.Kind) {
		return false
	}
	
	// Object name filtering
	if len(filters.ObjectNames) > 0 && !t.containsString(filters.ObjectNames, event.InvolvedObject.Name) {
		return false
	}
	
	// Source component filtering
	if len(filters.SourceComponents) > 0 && !t.containsString(filters.SourceComponents, event.Source.Component) {
		return false
	}
	
	return true
}

// passesContentFilters checks if an event row passes content filters
func (t *KubernetesEventsTable) passesContentFilters(row *EventRow, filters EventQueryFilters) bool {
	// Message pattern filtering
	if filters.MessagePattern != nil && !filters.MessagePattern.MatchString(row.Message) {
		return false
	}
	
	// Reason pattern filtering
	if filters.ReasonPattern != nil && !filters.ReasonPattern.MatchString(row.Reason) {
		return false
	}
	
	return true
}

// applyPostCollectionFilters applies filters that couldn't be applied during collection
func (t *KubernetesEventsTable) applyPostCollectionFilters(events []*EventRow, filters EventQueryFilters) []*EventRow {
	var filtered []*EventRow
	
	for _, event := range events {
		// Additional post-processing filters can be added here
		filtered = append(filtered, event)
	}
	
	// Apply final limit
	if filters.Limit != nil && int64(len(filtered)) > *filters.Limit {
		filtered = filtered[:*filters.Limit]
	}
	
	return filtered
}

// Utility methods

// matchesNamespaceFilter checks if a namespace matches the filter
func (t *KubernetesEventsTable) matchesNamespaceFilter(namespace string, allowedNamespaces []string) bool {
	for _, allowed := range allowedNamespaces {
		if allowed == "*" || allowed == namespace {
			return true
		}
	}
	return false
}

// containsString checks if a slice contains a string
func (t *KubernetesEventsTable) containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// logFiltersString creates a string representation of filters for logging
func (t *KubernetesEventsTable) logFiltersString(filters EventQueryFilters) string {
	var parts []string
	
	if len(filters.Namespaces) > 0 {
		parts = append(parts, fmt.Sprintf("namespaces=%v", filters.Namespaces))
	}
	if len(filters.EventTypes) > 0 {
		parts = append(parts, fmt.Sprintf("event_types=%v", filters.EventTypes))
	}
	if len(filters.ObjectKinds) > 0 {
		parts = append(parts, fmt.Sprintf("object_kinds=%v", filters.ObjectKinds))
	}
	if filters.SinceTime != nil {
		parts = append(parts, fmt.Sprintf("since=%s", filters.SinceTime.Format(time.RFC3339)))
	}
	if filters.UntilTime != nil {
		parts = append(parts, fmt.Sprintf("until=%s", filters.UntilTime.Format(time.RFC3339)))
	}
	if filters.Watch {
		parts = append(parts, "watch=true")
	}
	
	return strings.Join(parts, ", ")
}

// Query interface methods

// QueryEventsWithOptions provides an interface for querying events with options
func (t *KubernetesEventsTable) QueryEventsWithOptions(ctx context.Context, options EventQueryOptions) ([]*EventRow, error) {
	filters := t.convertOptionsToFilters(options)
	return t.collectEventsWithFilters(ctx, filters)
}

// convertOptionsToFilters converts EventQueryOptions to EventQueryFilters
func (t *KubernetesEventsTable) convertOptionsToFilters(options EventQueryOptions) EventQueryFilters {
	filters := EventQueryFilters{
		SinceTime:        options.SinceTime,
		UntilTime:        options.UntilTime,
		Namespaces:       options.Namespaces,
		EventTypes:       options.EventTypes,
		ObjectKinds:      options.ObjectKinds,
		ObjectNames:      options.ObjectNames,
		SourceComponents: options.SourceComponents,
		Limit:           options.Limit,
		Watch:           options.Watch,
		WatchTimeout:    options.WatchTimeout,
	}
	
	// Parse since duration if provided
	if options.SinceDuration != "" && options.SinceTime == nil {
		if duration, err := time.ParseDuration(options.SinceDuration); err == nil {
			since := time.Now().Add(-duration)
			filters.SinceTime = &since
		}
	}
	
	// Parse message pattern
	if options.MessagePattern != "" {
		if regex, err := regexp.Compile(options.MessagePattern); err == nil {
			filters.MessagePattern = regex
		}
	}
	
	// Parse reason pattern
	if options.ReasonPattern != "" {
		if regex, err := regexp.Compile(options.ReasonPattern); err == nil {
			filters.ReasonPattern = regex
		}
	}
	
	// Parse label selectors
	if len(options.LabelSelectors) > 0 {
		selectorStr := strings.Join(options.LabelSelectors, ",")
		if selector, err := labels.Parse(selectorStr); err == nil {
			filters.LabelSelector = selector
		}
	}
	
	return filters
}

// Close cleans up resources
func (t *KubernetesEventsTable) Close() error {
	// Cancel all active watchers
	t.watchersMutex.Lock()
	for key, cancelFunc := range t.watchers {
		cancelFunc()
		delete(t.watchers, key)
	}
	t.watchersMutex.Unlock()
	
	// Clear event cache
	t.cacheMutex.Lock()
	t.eventCache = make(map[string]*EventRow)
	t.cacheMutex.Unlock()
	
	if t.client != nil {
		return t.client.Close()
	}
	return nil
}

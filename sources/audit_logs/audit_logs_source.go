package audit_logs

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jlgore/tailpipe-plugin-kubernetes/config"
	"github.com/jlgore/tailpipe-plugin-kubernetes/internal"
	"github.com/jlgore/tailpipe-plugin-kubernetes/kclient"
	k8stables "github.com/jlgore/tailpipe-plugin-kubernetes/kubernetes"

	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const KubernetesAuditLogsSourceIdentifier = "kubernetes_audit_logs"

// KubernetesAuditLogsSource implements a RowSource that emits Kubernetes audit log rows.
// It reuses the existing table parsing/collection helpers to gather entries from files,
// API server pod logs, or a DaemonSet, depending on environment.
type KubernetesAuditLogsSource struct {
	row_source.RowSourceImpl[*KubernetesAuditLogsSourceConfig, *config.Config]

	client internal.KubernetesClientInterface
	// cached table helper to reuse parsing/collection logic
	tbl *k8stables.KubernetesAuditLogsTable

	errors []error
}

// Register the source in an init func
func init() {
	row_source.RegisterRowSource[*KubernetesAuditLogsSource]()
}

// Identifier returns the source identifier used in table metadata and configs.
func (*KubernetesAuditLogsSource) Identifier() string { return KubernetesAuditLogsSourceIdentifier }

// Init configures the source and sets up collection state and clients.
func (s *KubernetesAuditLogsSource) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	// Use time range collection state for time-based dedupe and resume.
	s.NewCollectionStateFunc = func() collection_state.CollectionState { return collection_state.NewTimeRangeCollectionState() }
	// Set API-like granularity
	s.RowSourceImpl.GetGranularityFunc = func() time.Duration { return time.Millisecond }

	if err := s.RowSourceImpl.Init(ctx, params, opts...); err != nil {
		return err
	}

	// Build Kubernetes client if cluster access may be needed (apiserver/daemonset method or fallback)
	// We lazily create it on demand in Collect to avoid errors when only file access is required.
	s.tbl = k8stables.NewKubernetesAuditLogsTable()
	s.errors = nil
	return nil
}

// Collect discovers and emits audit log rows within the requested time window.
func (s *KubernetesAuditLogsSource) Collect(ctx context.Context) error {
	// Determine time window from collection state
	from := s.CollectionTimeRange.StartTime()
	to := s.CollectionTimeRange.EndTime()

	filters := k8stables.AuditLogQueryFilters{}
	// Only set SinceTime/UntilTime if non-zero
	if !from.IsZero() {
		filters.SinceTime = &from
	} else {
		// fallback to connection since_time if provided
		if dur := s.Connection.GetSinceTime(); dur != "" {
			if d, err := time.ParseDuration(dur); err == nil {
				st := time.Now().Add(-d)
				filters.SinceTime = &st
			}
		}
	}
	if !to.IsZero() {
		filters.UntilTime = &to
	}

	// Apply namespace filtering from connection config
	filters.Namespaces = s.Connection.GetNamespaceList()

	// Always set config to propagate cluster_name into rows
	s.tbl.SetClient(s.client, s.Connection)

	// If method not specified, determine ordering based on ApiOnly preference
	var rows []*k8stables.AuditLogRow
	var err error

	method := ""
	if s.Config != nil && s.Config.Method != nil {
		method = *s.Config.Method
	}

	// Allow override of default file paths
	if s.Config != nil && len(s.Config.AuditLogPaths) > 0 {
		s.tbl.SetAuditLogPaths(s.Config.AuditLogPaths)
	}

	switch method {
	case "file":
		rows, err = s.collectFromFiles(ctx, filters)
	case "apiserver":
		if err := s.ensureClient(ctx); err != nil {
			return err
		}
		rows, err = s.tbl.CollectFromAPIServerPods(ctx, filters)
	case "daemonset":
		if err := s.ensureClient(ctx); err != nil {
			return err
		}
		rows, err = s.tbl.CollectFromDaemonSet(ctx, filters)
	default:
		if s.Config != nil && s.Config.ApiOnly {
			// API-only auto-detect: apiserver -> daemonset
			if err := s.ensureClient(ctx); err == nil {
				rows, err = s.tbl.CollectFromAPIServerPods(ctx, filters)
				if err != nil || len(rows) == 0 {
					dsRows, dsErr := s.tbl.CollectFromDaemonSet(ctx, filters)
					if dsErr == nil {
						rows = dsRows
					} else {
						err = dsErr
					}
				}
			}
		} else {
			// Default auto-detect: file -> apiserver -> daemonset
			rows, err = s.tbl.CollectFromFiles(ctx, filters)
			if err != nil || len(rows) == 0 {
				if err := s.ensureClient(ctx); err == nil {
					rows, err = s.tbl.CollectFromAPIServerPods(ctx, filters)
					if err != nil || len(rows) == 0 {
						dsRows, dsErr := s.tbl.CollectFromDaemonSet(ctx, filters)
						if dsErr == nil {
							rows = dsRows
						} else {
							err = dsErr
						}
					}
				}
			}
		}
	}

	if err != nil {
		return err
	}

	// Stream rows through SDK with collection state tracking
	clusterName := s.Connection.GetClusterName()
	for _, r := range rows {
		// Compute timestamp for state window (prefer stage timestamp)
		var ts time.Time
		if r.StageTimestamp != nil {
			ts = *r.StageTimestamp
		} else if r.RequestReceivedTime != nil {
			ts = *r.RequestReceivedTime
		}
		if ts.IsZero() {
			// if missing, fall back to from to avoid dropping due to time window
			ts = from
		}

		// Use audit_id as unique identity
		id := r.AuditID
		if id == "" {
			// fallback identity if audit id missing
			id = fmt.Sprintf("%s|%s|%s|%s|%s|%s", r.Verb, r.Resource, r.Namespace, r.Name, r.RequestURI, ts.UTC().Format(time.RFC3339Nano))
		}

		// Check collection state and update
		if !s.CollectionState.ShouldCollect(id, ts) {
			continue
		}

		// Build source enrichment
		srcName := clusterName
		if srcName == "" {
			srcName = KubernetesAuditLogsSourceIdentifier
		}
		enrichment := &schema.SourceEnrichment{CommonFields: schema.CommonFields{
			TpSourceType: KubernetesAuditLogsSourceIdentifier,
			TpSourceName: &srcName,
		}}

		row := &types.RowData{Data: r, SourceEnrichment: enrichment}

		if err := s.CollectionState.OnCollected(id, ts); err != nil {
			s.errors = append(s.errors, fmt.Errorf("state error for %s: %w", id, err))
			continue
		}

		if err := s.OnRow(ctx, row); err != nil {
			s.errors = append(s.errors, fmt.Errorf("emit error for %s: %w", id, err))
			continue
		}
	}

	if len(s.errors) > 0 {
		slog.Warn("audit logs collection completed with errors", "count", len(s.errors))
		return fmt.Errorf("encountered %d errors during audit logs collection", len(s.errors))
	}
	return nil
}

// Wrappers to reuse table helper methods (exposed via method names with upper-case)
func (s *KubernetesAuditLogsSource) collectFromFiles(ctx context.Context, filters k8stables.AuditLogQueryFilters) ([]*k8stables.AuditLogRow, error) {
	return s.tbl.CollectFromFiles(ctx, filters)
}

// ensureClient creates a Kubernetes client if required and attaches it to the table
func (s *KubernetesAuditLogsSource) ensureClient(ctx context.Context) error {
	if s.client != nil {
		return nil
	}
	c, err := kclient.NewKubernetesClient(s.Connection)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}
	s.client = c
	s.tbl.SetClient(s.client, s.Connection)
	if testErr := s.client.TestConnection(ctx); testErr != nil {
		slog.Debug("k8s client test failed", "error", testErr)
	}
	return nil
}

package events

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/jlgore/tailpipe-plugin-kubernetes/config"
	"github.com/jlgore/tailpipe-plugin-kubernetes/kclient"
	k8stables "github.com/jlgore/tailpipe-plugin-kubernetes/kubernetes"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const KubernetesEventsSourceIdentifier = "kubernetes_events"

// KubernetesEventsSource emits EventRow from the Kubernetes API
type KubernetesEventsSource struct {
	row_source.RowSourceImpl[*KubernetesEventsSourceConfig, *config.Config]
	tbl *k8stables.KubernetesEventsTable
}

func init() { row_source.RegisterRowSource[*KubernetesEventsSource]() }

func (*KubernetesEventsSource) Identifier() string { return KubernetesEventsSourceIdentifier }

func (s *KubernetesEventsSource) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	s.NewCollectionStateFunc = func() collection_state.CollectionState { return collection_state.NewTimeRangeCollectionState() }
	s.RowSourceImpl.GetGranularityFunc = func() time.Duration { return time.Millisecond }
	if err := s.RowSourceImpl.Init(ctx, params, opts...); err != nil {
		return err
	}
	s.tbl = k8stables.NewKubernetesEventsTable()
	return nil
}

func (s *KubernetesEventsSource) Collect(ctx context.Context) error {
	client, err := kclient.NewKubernetesClient(s.Connection)
	if err != nil {
		return err
	}
	s.tbl.SetClient(client, s.Connection)

	// Build filters
	filters := k8stables.EventQueryFilters{}
	from := s.CollectionTimeRange.StartTime()
	to := s.CollectionTimeRange.EndTime()
	if !from.IsZero() {
		filters.SinceTime = &from
	}
	if !to.IsZero() {
		filters.UntilTime = &to
	}
	filters.Namespaces = s.Connection.GetNamespaceList()
	filters.EventTypes = s.Config.EventTypes
	filters.ObjectKinds = s.Config.ObjectKinds
	filters.ObjectNames = s.Config.ObjectNames
	filters.SourceComponents = s.Config.SourceComponents
	filters.Limit = s.Config.Limit
	filters.Watch = s.Config.Watch
	filters.WatchTimeout = s.Config.WatchTimeout

	if s.Config.MessagePattern != "" {
		if re, err := regexp.Compile(s.Config.MessagePattern); err == nil {
			filters.MessagePattern = re
		}
	}
	if s.Config.ReasonPattern != "" {
		if re, err := regexp.Compile(s.Config.ReasonPattern); err == nil {
			filters.ReasonPattern = re
		}
	}

	// Collect events
	rows, err := s.tbl.CollectWithFilters(ctx, filters)
	if err != nil {
		return err
	}

	// Emit rows with collection state
	srcName := s.Connection.GetClusterName()
	if srcName == "" {
		srcName = KubernetesEventsSourceIdentifier
	}
	for _, r := range rows {
		ts := time.Time{}
		if r.Timestamp != nil {
			ts = *r.Timestamp
		}
		if ts.IsZero() {
			if r.LastTimestamp != nil {
				ts = *r.LastTimestamp
			} else if r.FirstTimestamp != nil {
				ts = *r.FirstTimestamp
			} else {
				ts = from
			}
		}

		// prefer deduplication key if present
		id := r.DeduplicationKey
		if id == "" {
			id = strings.Join([]string{r.Namespace, r.EventType, r.Reason, r.ObjectKind, r.ObjectName, ts.UTC().Format(time.RFC3339Nano)}, "|")
		}
		if !s.CollectionState.ShouldCollect(id, ts) {
			continue
		}
		if err := s.CollectionState.OnCollected(id, ts); err != nil {
			continue
		}

		enrichment := &schema.SourceEnrichment{CommonFields: schema.CommonFields{TpSourceType: KubernetesEventsSourceIdentifier, TpSourceName: &srcName}}
		if err := s.OnRow(ctx, &types.RowData{Data: r, SourceEnrichment: enrichment}); err != nil {
			continue
		}
	}
	return nil
}

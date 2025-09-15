package pod_logs

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

const KubernetesPodLogsSourceIdentifier = "kubernetes_pod_logs"

// KubernetesPodLogsSource emits PodLogRow by querying Kubernetes pod/container logs
type KubernetesPodLogsSource struct {
	row_source.RowSourceImpl[*KubernetesPodLogsSourceConfig, *config.Config]

	tbl *k8stables.KubernetesPodLogsTable
}

func init() { row_source.RegisterRowSource[*KubernetesPodLogsSource]() }

func (*KubernetesPodLogsSource) Identifier() string { return KubernetesPodLogsSourceIdentifier }

func (s *KubernetesPodLogsSource) Init(ctx context.Context, params *row_source.RowSourceParams, opts ...row_source.RowSourceOption) error {
	s.NewCollectionStateFunc = func() collection_state.CollectionState { return collection_state.NewTimeRangeCollectionState() }
	s.RowSourceImpl.GetGranularityFunc = func() time.Duration { return time.Millisecond }
	if err := s.RowSourceImpl.Init(ctx, params, opts...); err != nil {
		return err
	}
	s.tbl = k8stables.NewKubernetesPodLogsTable()
	return nil
}

func (s *KubernetesPodLogsSource) Collect(ctx context.Context) error {
	// Ensure client
	client, err := kclient.NewKubernetesClient(s.Connection)
	if err != nil {
		return err
	}
	// Attach client and config to table
	s.tbl.SetClient(client, s.Connection)

	// Build filters using connection and source config
	filters := k8stables.LogQueryFilters{}
	from := s.CollectionTimeRange.StartTime()
	to := s.CollectionTimeRange.EndTime()
	if !from.IsZero() {
		filters.SinceTime = &from
	}
	if !to.IsZero() {
		filters.UntilTime = &to
	}

	filters.Namespaces = s.Connection.GetNamespaceList()
	if len(s.Config.LogLevels) > 0 {
		filters.LogLevels = s.Config.LogLevels
	}
	if s.Config.TailLines != nil {
		filters.TailLines = s.Config.TailLines
	}
	if s.Config.LimitBytes != nil {
		filters.Limit = s.Config.LimitBytes
	}
	if s.Config.PodNamePattern != "" {
		if re, err := regexp.Compile(s.Config.PodNamePattern); err == nil {
			filters.PodNamePattern = re
		}
	}
	if len(s.Config.LabelSelectors) > 0 {
		// table combines with AND using comma
		// we defer label selector parsing to the table using strings.Join
	}

	rows, err := s.tbl.CollectWithFilters(ctx, filters)
	if err != nil {
		return err
	}

	// Emit rows with enrichment and collection state
	srcName := s.Connection.GetClusterName()
	if srcName == "" {
		srcName = KubernetesPodLogsSourceIdentifier
	}
	for _, r := range rows {
		ts := time.Time{}
		if r.Timestamp != nil {
			ts = *r.Timestamp
		}
		if ts.IsZero() {
			ts = from
		}

		id := strings.Join([]string{r.Namespace, r.PodName, r.ContainerName, ts.UTC().Format(time.RFC3339Nano)}, "|")
		if !s.CollectionState.ShouldCollect(id, ts) {
			continue
		}

		if err := s.CollectionState.OnCollected(id, ts); err != nil {
			continue
		}

		enrichment := &schema.SourceEnrichment{CommonFields: schema.CommonFields{TpSourceType: KubernetesPodLogsSourceIdentifier, TpSourceName: &srcName}}
		if err := s.OnRow(ctx, &types.RowData{Data: r, SourceEnrichment: enrichment}); err != nil {
			continue
		}
	}

	return nil
}

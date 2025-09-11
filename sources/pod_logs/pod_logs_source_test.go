package pod_logs

import (
    "context"
    "testing"
    "time"

    "github.com/hashicorp/hcl/v2"
    "github.com/turbot/tailpipe-plugin-sdk/row_source"
    "github.com/turbot/tailpipe-plugin-sdk/types"
)

func TestPodLogsSourceConfigValidate(t *testing.T) {
    c := &KubernetesPodLogsSourceConfig{ TailLines: int64Ptr(-1) }
    if err := c.Validate(); err == nil {
        t.Fatalf("expected error for negative tail_lines")
    }
    b := int64(-1)
    c = &KubernetesPodLogsSourceConfig{ LimitBytes: &b }
    if err := c.Validate(); err == nil {
        t.Fatalf("expected error for negative limit_bytes")
    }
}

func TestPodLogsSourceIdentifier(t *testing.T) {
    s := &KubernetesPodLogsSource{}
    if id := s.Identifier(); id != KubernetesPodLogsSourceIdentifier {
        t.Fatalf("unexpected identifier %s", id)
    }
}

func TestPodLogsSourceInit(t *testing.T) {
    s := &KubernetesPodLogsSource{}

    // minimal source HCL
    hclSrc := []byte("")
    srcData := types.NewSourceConfigData(hclSrc, hcl.Range{}, KubernetesPodLogsSourceIdentifier)
    // connection: service_account=true to bypass kubeconfig existence validation
    hclConn := []byte("service_account = true\nnamespace = \"*\"")
    connData := types.NewConnectionConfigData(hclConn, hcl.Range{}, "kubernetes")

    params := &row_source.RowSourceParams{
        SourceConfigData:    srcData,
        ConnectionData:      connData,
        CollectionStatePath: "",
        From:                time.Now().Add(-15 * time.Minute),
        To:                  time.Now(),
        CollectionTempDir:   t.TempDir(),
    }

    if err := s.Init(context.Background(), params); err != nil {
        t.Fatalf("Init failed: %v", err)
    }
}

func int64Ptr(v int64) *int64 { return &v }


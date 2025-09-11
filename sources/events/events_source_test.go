package events

import (
    "context"
    "testing"
    "time"

    "github.com/hashicorp/hcl/v2"
    "github.com/turbot/tailpipe-plugin-sdk/row_source"
    "github.com/turbot/tailpipe-plugin-sdk/types"
)

func TestEventsSourceIdentifier(t *testing.T) {
    s := &KubernetesEventsSource{}
    if id := s.Identifier(); id != KubernetesEventsSourceIdentifier {
        t.Fatalf("unexpected identifier %s", id)
    }
}

func TestEventsSourceInit(t *testing.T) {
    s := &KubernetesEventsSource{}

    hclSrc := []byte("")
    srcData := types.NewSourceConfigData(hclSrc, hcl.Range{}, KubernetesEventsSourceIdentifier)

    // connection: service_account=true to bypass kubeconfig existence validation
    hclConn := []byte("service_account = true\nnamespace = \"*\"")
    connData := types.NewConnectionConfigData(hclConn, hcl.Range{}, "kubernetes")

    params := &row_source.RowSourceParams{
        SourceConfigData:    srcData,
        ConnectionData:      connData,
        CollectionStatePath: "",
        From:                time.Now().Add(-30 * time.Minute),
        To:                  time.Now(),
        CollectionTempDir:   t.TempDir(),
    }

    if err := s.Init(context.Background(), params); err != nil {
        t.Fatalf("Init failed: %v", err)
    }
}


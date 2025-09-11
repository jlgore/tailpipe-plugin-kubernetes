package audit_logs

import (
    "context"
    "testing"
    "time"

    "github.com/hashicorp/hcl/v2"
    "github.com/turbot/tailpipe-plugin-sdk/row_source"
    "github.com/turbot/tailpipe-plugin-sdk/types"
)

func TestAuditLogsSourceConfigValidate(t *testing.T) {
    // valid: empty method
    c := &KubernetesAuditLogsSourceConfig{}
    if err := c.Validate(); err != nil {
        t.Fatalf("expected no error, got %v", err)
    }

    // valid: file
    m := "file"
    c = &KubernetesAuditLogsSourceConfig{ Method: &m }
    if err := c.Validate(); err != nil {
        t.Fatalf("expected no error for method=file, got %v", err)
    }

    // invalid method
    bad := "not_a_method"
    c = &KubernetesAuditLogsSourceConfig{ Method: &bad }
    if err := c.Validate(); err == nil {
        t.Fatalf("expected error for invalid method, got nil")
    }
}

func TestAuditLogsSourceIdentifier(t *testing.T) {
    s := &KubernetesAuditLogsSource{}
    if id := s.Identifier(); id != KubernetesAuditLogsSourceIdentifier {
        t.Fatalf("unexpected identifier %s", id)
    }
}

func TestAuditLogsSourceInit(t *testing.T) {
    s := &KubernetesAuditLogsSource{}

    // source HCL: use file method to avoid cluster dependency
    hclSrc := []byte("method = \"file\"")
    srcData := types.NewSourceConfigData(hclSrc, hcl.Range{}, KubernetesAuditLogsSourceIdentifier)

    // connection HCL: service_account=true to avoid kubeconfig dependency during validation
    hclConn := []byte("service_account = true\nnamespace = \"*\"")
    connData := types.NewConnectionConfigData(hclConn, hcl.Range{}, "kubernetes")

    params := &row_source.RowSourceParams{
        SourceConfigData:    srcData,
        ConnectionData:      connData,
        CollectionStatePath: "",
        From:                time.Now().Add(-1 * time.Hour),
        To:                  time.Now(),
        CollectionTempDir:   t.TempDir(),
    }

    if err := s.Init(context.Background(), params); err != nil {
        t.Fatalf("Init failed: %v", err)
    }
}

func TestAuditLogsSourceCollect_FileNoAccess(t *testing.T) {
    // This test verifies Collect returns an error when no audit log files are accessible.
    s := &KubernetesAuditLogsSource{}

    hclSrc := []byte("method = \"file\"")
    srcData := types.NewSourceConfigData(hclSrc, hcl.Range{}, KubernetesAuditLogsSourceIdentifier)
    hclConn := []byte("service_account = true\nnamespace = \"*\"")
    connData := types.NewConnectionConfigData(hclConn, hcl.Range{}, "kubernetes")

    params := &row_source.RowSourceParams{
        SourceConfigData:    srcData,
        ConnectionData:      connData,
        CollectionStatePath: "",
        From:                time.Now().Add(-1 * time.Hour),
        To:                  time.Now(),
        CollectionTempDir:   t.TempDir(),
    }

    if err := s.Init(context.Background(), params); err != nil {
        t.Fatalf("Init failed: %v", err)
    }

    // No files likely exist in default paths in test env → expect error
    if err := s.Collect(context.Background()); err == nil {
        t.Fatalf("expected error collecting via file method with no accessible audit logs, got nil")
    }
}


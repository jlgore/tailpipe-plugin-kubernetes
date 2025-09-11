package audit_logs

import (
    "context"
    "encoding/json"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/hashicorp/hcl/v2"
    "github.com/turbot/tailpipe-plugin-kubernetes/kubernetes"
    "github.com/turbot/tailpipe-plugin-sdk/context_values"
    sdkevents "github.com/turbot/tailpipe-plugin-sdk/events"
    "github.com/turbot/tailpipe-plugin-sdk/row_source"
    "github.com/turbot/tailpipe-plugin-sdk/types"
)

// observer to capture rows emitted by the source
type rowCaptureObserver struct{ rows []*kubernetes.AuditLogRow }
func (o *rowCaptureObserver) Notify(_ context.Context, ev sdkevents.Event) error {
    if r, ok := ev.(*sdkevents.RowExtracted); ok {
        if row, ok2 := r.Row.(*kubernetes.AuditLogRow); ok2 {
            o.rows = append(o.rows, row)
        }
    }
    return nil
}

func writeAuditLines(t *testing.T, path string, count int) {
    f, err := os.Create(path)
    if err != nil { t.Fatalf("create file: %v", err) }
    defer f.Close()
    now := time.Now().UTC()
    for i := 0; i < count; i++ {
        evt := map[string]any{
            "kind":           "Event",
            "apiVersion":     "audit.k8s.io/v1",
            "level":          "Metadata",
            "auditID":        "test-audit-id-" + time.Now().Format("150405.000") + "-" + string(rune('a'+i)),
            "stage":          "ResponseComplete",
            "requestURI":     "/api/v1/namespaces/default/pods",
            "verb":           "list",
            "user":           map[string]any{"username": "test-user"},
            "sourceIPs":      []string{"127.0.0.1"},
            "userAgent":      "kubectl/v1.29 (linux/amd64)",
            "stageTimestamp": now.Format(time.RFC3339Nano),
            "responseStatus": map[string]any{"code": 200, "status": "Success"},
            "objectRef":      map[string]any{"namespace": "default", "resource": "pods"},
        }
        b, _ := json.Marshal(evt)
        if _, err := f.Write(append(b, '\n')); err != nil { t.Fatalf("write: %v", err) }
    }
}

func TestAuditLogsIngestFromFile(t *testing.T) {
    dir := t.TempDir()
    file := filepath.Join(dir, "audit.jsonl")
    writeAuditLines(t, file, 2)

    // configure source to read from our file
    hclSrc := []byte("method = \"file\"\n" + "audit_log_paths = [\"" + file + "\"]\n")
    srcData := types.NewSourceConfigData(hclSrc, hcl.Range{}, KubernetesAuditLogsSourceIdentifier)
    hclConn := []byte("service_account = true\ncluster_name=\"test-cluster\"\nnamespace = \"*\"\n")
    connData := types.NewConnectionConfigData(hclConn, hcl.Range{}, "kubernetes")

    params := &row_source.RowSourceParams{
        SourceConfigData:    srcData,
        ConnectionData:      connData,
        CollectionStatePath: filepath.Join(dir, "state.json"),
        From:                time.Now().Add(-2 * time.Hour),
        To:                  time.Now(),
        CollectionTempDir:   filepath.Join(dir, "tmp"),
    }

    s := &KubernetesAuditLogsSource{}
    if err := s.Init(context.Background(), params); err != nil {
        t.Fatalf("Init failed: %v", err)
    }

    // add observer to capture rows
    rc := &rowCaptureObserver{}
    if err := s.AddObserver(rc); err != nil { t.Fatalf("AddObserver: %v", err) }

    ctx := context_values.WithExecutionId(context.Background(), "test-exec")
    if err := s.Collect(ctx); err != nil {
        t.Fatalf("Collect failed: %v", err)
    }

    if len(rc.rows) != 2 {
        t.Fatalf("expected 2 rows, got %d", len(rc.rows))
    }
    // sanity-check some fields
    if rc.rows[0].UserName != "test-user" || rc.rows[0].Verb != "list" || rc.rows[0].Resource != "pods" {
        t.Fatalf("unexpected row content: %+v", rc.rows[0])
    }
}

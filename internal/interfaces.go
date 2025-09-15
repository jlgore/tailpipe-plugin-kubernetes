package internal

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ListOptions represents options for listing Kubernetes resources
type ListOptions struct {
	LabelSelector  string
	FieldSelector  string
	Namespace      string
	TimeoutSeconds *int64
}

// LogOptions represents options for retrieving pod logs
type LogOptions struct {
	Follow       bool
	Previous     bool
	SinceTime    *metav1.Time
	SinceSeconds *int64
	Timestamps   bool
	TailLines    *int64
	LimitBytes   *int64
}

// EventWatcher represents a Kubernetes event watcher
type EventWatcher interface {
	// Channel returns the channel where events are sent
	ResultChan() <-chan EventWatchResult
	// Stop stops the watcher
	Stop()
}

// EventWatchResult represents a single event watch result
type EventWatchResult struct {
	Type   string // ADDED, MODIFIED, DELETED
	Object *corev1.Event
	Error  error
}

// KubernetesClientInterface defines the interface for Kubernetes operations
type KubernetesClientInterface interface {
	// Pod operations
	GetPodsWithLogs(ctx context.Context, opts ListOptions) ([]corev1.Pod, error)
	GetPodLogs(ctx context.Context, namespace, podName, containerName string, opts LogOptions) (io.ReadCloser, error)

	// Event operations
	ListEvents(ctx context.Context, opts ListOptions) ([]corev1.Event, error)
	WatchEvents(ctx context.Context, opts ListOptions) (EventWatcher, error)

	// Resource operations
	ListResources(ctx context.Context, resourceType string, opts ListOptions) ([]runtime.Object, error)

	// Connection management
	TestConnection(ctx context.Context) error
	Close() error
}

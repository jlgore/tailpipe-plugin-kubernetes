package kclient

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jlgore/tailpipe-plugin-kubernetes/config"
	"github.com/jlgore/tailpipe-plugin-kubernetes/internal"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	// Import cloud provider authentication plugins
	_ "k8s.io/client-go/plugin/pkg/client/auth/exec"
)

// Ensure KubernetesClient implements the interface
var _ internal.KubernetesClientInterface = (*KubernetesClient)(nil)

// AuthMethod represents the authentication method used
type AuthMethod string

const (
	AuthMethodKubeconfig     AuthMethod = "kubeconfig"
	AuthMethodInCluster      AuthMethod = "in_cluster"
	AuthMethodServiceAccount AuthMethod = "service_account"
	AuthMethodDirect         AuthMethod = "direct"
	AuthMethodCloudProvider  AuthMethod = "cloud_provider"
)

// KubernetesClient wraps the Kubernetes client with configuration and additional functionality
type KubernetesClient struct {
	clientset    *kubernetes.Clientset
	config       *rest.Config
	pluginConfig *config.Config

	authMethod AuthMethod
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

// ListOptions represents options for listing resources
type ListOptions struct {
	LabelSelector  string
	FieldSelector  string
	Namespace      string
	TimeoutSeconds *int64
}

// NewKubernetesClient creates a new Kubernetes client with automatic authentication detection
func NewKubernetesClient(cfg *config.Config) (*KubernetesClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	client := &KubernetesClient{pluginConfig: cfg}

	restConfig, authMethod, err := client.buildRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build REST config: %w", err)
	}
	client.config = restConfig
	client.authMethod = authMethod

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}
	client.clientset = clientset
	return client, nil
}

// buildRestConfig attempts multiple authentication methods in priority order
func (k *KubernetesClient) buildRestConfig() (*rest.Config, AuthMethod, error) {
	var lastErr error

	if !k.pluginConfig.GetServiceAccount() {
		if restConfig, err := k.tryKubeconfigAuth(); err == nil {
			return restConfig, AuthMethodKubeconfig, nil
		} else {
			lastErr = fmt.Errorf("kubeconfig auth failed: %w", err)
		}
	}

	if k.pluginConfig.GetServiceAccount() || k.isRunningInCluster() {
		if restConfig, err := k.tryInClusterAuth(); err == nil {
			return restConfig, AuthMethodInCluster, nil
		} else {
			lastErr = fmt.Errorf("in-cluster auth failed: %w", err)
		}
	}

	if serverURL := k.pluginConfig.GetServerURL(); serverURL != "" {
		if restConfig, err := k.tryDirectAuth(); err == nil {
			return restConfig, AuthMethodDirect, nil
		} else {
			lastErr = fmt.Errorf("direct auth failed: %w", err)
		}
	}

	if k.pluginConfig.GetServiceAccount() {
		if restConfig, err := k.tryKubeconfigAuth(); err == nil {
			return restConfig, AuthMethodKubeconfig, nil
		} else {
			lastErr = fmt.Errorf("fallback kubeconfig auth failed: %w", err)
		}
	}

	return nil, "", fmt.Errorf("all authentication methods failed, last error: %w", lastErr)
}

func (k *KubernetesClient) tryKubeconfigAuth() (*rest.Config, error) {
	kubeconfigPath := k.pluginConfig.GetKubeconfigPath()
	if _, err := os.Stat(kubeconfigPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("kubeconfig file not found at %s", kubeconfigPath)
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath
	configOverrides := &clientcmd.ConfigOverrides{}
	if context := k.pluginConfig.GetKubeconfigContext(); context != "" {
		configOverrides.CurrentContext = context
	}
	if serverURL := k.pluginConfig.GetServerURL(); serverURL != "" {
		configOverrides.ClusterInfo = api.Cluster{Server: serverURL}
	}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	restConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create REST config from kubeconfig: %w", err)
	}
	k.configureRestConfig(restConfig)
	return restConfig, nil
}

func (k *KubernetesClient) tryInClusterAuth() (*rest.Config, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}
	if serverURL := k.pluginConfig.GetServerURL(); serverURL != "" {
		restConfig.Host = serverURL
	}
	k.configureRestConfig(restConfig)
	return restConfig, nil
}

func (k *KubernetesClient) tryDirectAuth() (*rest.Config, error) {
	serverURL := k.pluginConfig.GetServerURL()
	if serverURL == "" {
		return nil, fmt.Errorf("server URL is required for direct authentication")
	}
	restConfig := &rest.Config{Host: serverURL}
	k.configureRestConfig(restConfig)
	return restConfig, nil
}

func (k *KubernetesClient) configureRestConfig(restConfig *rest.Config) {
	restConfig.Timeout = 30 * time.Second
	restConfig.UserAgent = "tailpipe-plugin-kubernetes/1.0"
	restConfig.QPS = 50
	restConfig.Burst = 100
	restConfig.WarningHandler = rest.NoWarnings{}
}

func (k *KubernetesClient) isRunningInCluster() bool {
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		return true
	}
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" && os.Getenv("KUBERNETES_SERVICE_PORT") != "" {
		return true
	}
	return false
}

func (k *KubernetesClient) TestConnection(ctx context.Context) error {
	_, err := k.clientset.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}
	_, err = k.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("permission test failed (cannot list namespaces): %w", err)
	}
	return nil
}

func (k *KubernetesClient) GetClientInfo(ctx context.Context) (map[string]interface{}, error) {
	info := map[string]interface{}{"auth_method": string(k.authMethod)}
	if version, err := k.clientset.Discovery().ServerVersion(); err == nil {
		info["server_version"] = version.String()
	}
	if k.authMethod == AuthMethodKubeconfig {
		if currentContext, err := k.getCurrentContext(); err == nil {
			info["current_context"] = currentContext
		}
	}
	return info, nil
}

func (k *KubernetesClient) getCurrentContext() (string, error) {
	kubeconfigPath := k.pluginConfig.GetKubeconfigPath()
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	rawConfig, err := kubeConfig.RawConfig()
	if err != nil {
		return "", err
	}
	return rawConfig.CurrentContext, nil
}

func (k *KubernetesClient) getNamespaces(ctx context.Context) ([]string, error) {
	namespaceList := k.pluginConfig.GetNamespaceList()
	if len(namespaceList) == 1 && namespaceList[0] == "*" {
		nsList, err := k.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to list namespaces: %w", err)
		}
		var namespaces []string
		for _, ns := range nsList.Items {
			namespaces = append(namespaces, ns.Name)
		}
		return namespaces, nil
	}
	return namespaceList, nil
}

func (k *KubernetesClient) ListPods(ctx context.Context, opts internal.ListOptions) ([]corev1.Pod, error) {
	var allPods []corev1.Pod
	namespaces := []string{opts.Namespace}
	if opts.Namespace == "" || opts.Namespace == "*" {
		var err error
		namespaces, err = k.getNamespaces(ctx)
		if err != nil {
			return nil, err
		}
	}
	for _, namespace := range namespaces {
		listOpts := metav1.ListOptions{LabelSelector: opts.LabelSelector, FieldSelector: opts.FieldSelector}
		if opts.TimeoutSeconds != nil {
			listOpts.TimeoutSeconds = opts.TimeoutSeconds
		}
		pods, err := k.clientset.CoreV1().Pods(namespace).List(ctx, listOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to list pods in namespace %s: %w", namespace, err)
		}
		allPods = append(allPods, pods.Items...)
	}
	return allPods, nil
}

func (k *KubernetesClient) GetPodLogs(ctx context.Context, namespace, podName, containerName string, opts internal.LogOptions) (io.ReadCloser, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}
	if podName == "" {
		return nil, fmt.Errorf("pod name cannot be empty")
	}
	logOpts := &corev1.PodLogOptions{Container: containerName, Follow: opts.Follow, Previous: opts.Previous, Timestamps: opts.Timestamps}
	if opts.SinceTime != nil {
		logOpts.SinceTime = opts.SinceTime
	}
	if opts.SinceSeconds != nil {
		logOpts.SinceSeconds = opts.SinceSeconds
	}
	if opts.TailLines != nil {
		logOpts.TailLines = opts.TailLines
	}
	if opts.LimitBytes != nil {
		logOpts.LimitBytes = opts.LimitBytes
	}
	req := k.clientset.CoreV1().Pods(namespace).GetLogs(podName, logOpts)
	return req.Stream(ctx)
}

func (k *KubernetesClient) ListEvents(ctx context.Context, opts internal.ListOptions) ([]corev1.Event, error) {
	var allEvents []corev1.Event
	namespaces := []string{opts.Namespace}
	if opts.Namespace == "" || opts.Namespace == "*" {
		var err error
		namespaces, err = k.getNamespaces(ctx)
		if err != nil {
			return nil, err
		}
	}
	for _, namespace := range namespaces {
		listOpts := metav1.ListOptions{LabelSelector: opts.LabelSelector, FieldSelector: opts.FieldSelector}
		if opts.TimeoutSeconds != nil {
			listOpts.TimeoutSeconds = opts.TimeoutSeconds
		}
		events, err := k.clientset.CoreV1().Events(namespace).List(ctx, listOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to list events in namespace %s: %w", namespace, err)
		}
		allEvents = append(allEvents, events.Items...)
	}
	return allEvents, nil
}

func (k *KubernetesClient) WatchEvents(ctx context.Context, opts internal.ListOptions) (internal.EventWatcher, error) {
	listOpts := metav1.ListOptions{LabelSelector: opts.LabelSelector, FieldSelector: opts.FieldSelector, Watch: true}
	if opts.TimeoutSeconds != nil {
		listOpts.TimeoutSeconds = opts.TimeoutSeconds
	}
	watcher, err := k.clientset.CoreV1().Events(opts.Namespace).Watch(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to watch events: %w", err)
	}
	return &eventWatcherAdapter{w: watcher}, nil
}

type eventWatcherAdapter struct{ w watch.Interface }

func (e *eventWatcherAdapter) ResultChan() <-chan internal.EventWatchResult {
	out := make(chan internal.EventWatchResult)
	ch := e.w.ResultChan()
	go func() {
		defer close(out)
		for item := range ch {
			var ev *corev1.Event
			if event, ok := item.Object.(*corev1.Event); ok {
				ev = event
			}
			out <- internal.EventWatchResult{Type: string(item.Type), Object: ev}
		}
	}()
	return out
}
func (e *eventWatcherAdapter) Stop() { e.w.Stop() }

func (k *KubernetesClient) ListResources(ctx context.Context, resourceType string, opts internal.ListOptions) ([]runtime.Object, error) {
	var allResources []runtime.Object
	namespaces := []string{opts.Namespace}
	if opts.Namespace == "" || opts.Namespace == "*" {
		var err error
		namespaces, err = k.getNamespaces(ctx)
		if err != nil {
			return nil, err
		}
	}
	listOpts := metav1.ListOptions{LabelSelector: opts.LabelSelector, FieldSelector: opts.FieldSelector}
	if opts.TimeoutSeconds != nil {
		listOpts.TimeoutSeconds = opts.TimeoutSeconds
	}
	for _, namespace := range namespaces {
		switch strings.ToLower(resourceType) {
		case "pod", "pods":
			pods, err := k.clientset.CoreV1().Pods(namespace).List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to list pods in namespace %s: %w", namespace, err)
			}
			for i := range pods.Items {
				allResources = append(allResources, &pods.Items[i])
			}
		case "service", "services", "svc":
			services, err := k.clientset.CoreV1().Services(namespace).List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to list services in namespace %s: %w", namespace, err)
			}
			for i := range services.Items {
				allResources = append(allResources, &services.Items[i])
			}
		case "deployment", "deployments", "deploy":
			deployments, err := k.clientset.AppsV1().Deployments(namespace).List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to list deployments in namespace %s: %w", namespace, err)
			}
			for i := range deployments.Items {
				allResources = append(allResources, &deployments.Items[i])
			}
		case "replicaset", "replicasets", "rs":
			replicasets, err := k.clientset.AppsV1().ReplicaSets(namespace).List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to list replicasets in namespace %s: %w", namespace, err)
			}
			for i := range replicasets.Items {
				allResources = append(allResources, &replicasets.Items[i])
			}
		case "configmap", "configmaps", "cm":
			configmaps, err := k.clientset.CoreV1().ConfigMaps(namespace).List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to list configmaps in namespace %s: %w", namespace, err)
			}
			for i := range configmaps.Items {
				allResources = append(allResources, &configmaps.Items[i])
			}
		case "secret", "secrets":
			secrets, err := k.clientset.CoreV1().Secrets(namespace).List(ctx, listOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to list secrets in namespace %s: %w", namespace, err)
			}
			for i := range secrets.Items {
				allResources = append(allResources, &secrets.Items[i])
			}
		default:
			return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
		}
	}
	return allResources, nil
}

func (k *KubernetesClient) GetPodsWithLogs(ctx context.Context, opts internal.ListOptions) ([]corev1.Pod, error) {
	pods, err := k.ListPods(ctx, opts)
	if err != nil {
		return nil, err
	}
	var podsWithLogs []corev1.Pod
	for _, pod := range pods {
		hasLogs := false
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.State.Running != nil || containerStatus.State.Terminated != nil {
				hasLogs = true
				break
			}
		}
		if hasLogs {
			podsWithLogs = append(podsWithLogs, pod)
		}
	}
	return podsWithLogs, nil
}

func (k *KubernetesClient) GetNamespaceEvents(ctx context.Context, namespace string, opts internal.ListOptions) ([]corev1.Event, error) {
	fieldSelector := opts.FieldSelector
	if fieldSelector != "" {
		fieldSelector += ","
	}
	fieldSelector += "involvedObject.namespace=" + namespace
	newOpts := internal.ListOptions{LabelSelector: opts.LabelSelector, FieldSelector: fieldSelector, Namespace: namespace, TimeoutSeconds: opts.TimeoutSeconds}
	return k.ListEvents(ctx, newOpts)
}

func (k *KubernetesClient) Close() error { return nil }

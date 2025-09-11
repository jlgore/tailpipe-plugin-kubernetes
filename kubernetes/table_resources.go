package kubernetes

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-kubernetes/config"
	"github.com/turbot/tailpipe-plugin-kubernetes/internal"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// ResourceRow represents a single Kubernetes resource
type ResourceRow struct {
	// Core resource fields (matching required schema)
	Namespace    string `json:"namespace"`
	ResourceType string `json:"resource_type"`  // Pod, Service, Deployment, etc.
	ResourceName string `json:"resource_name"`
	Status       string `json:"status"`
	CreatedAt    *time.Time `json:"created_at"`
	Labels       map[string]interface{} `json:"labels"`
	Annotations  map[string]interface{} `json:"annotations"`
	Spec         map[string]interface{} `json:"spec"`
	NodeName     string `json:"node_name"`     // For pods and node-specific resources
	ClusterName  string `json:"cluster_name"`
	
	// Extended metadata for resource relationships
	UID             string                 `json:"uid,omitempty"`
	OwnerReferences []OwnerReference       `json:"owner_references,omitempty"`
	ResourceVersion string                 `json:"resource_version,omitempty"`
	
	// Resource-specific status details
	StatusDetails   map[string]interface{} `json:"status_details,omitempty"`
	Replicas        *ResourceReplicas      `json:"replicas,omitempty"`         // For deployments, statefulsets, etc.
	Conditions      []ResourceCondition    `json:"conditions,omitempty"`
	
	// Networking and storage fields
	ServiceType     string   `json:"service_type,omitempty"`      // For services
	ClusterIP       string   `json:"cluster_ip,omitempty"`        // For services
	ExternalIPs     []string `json:"external_ips,omitempty"`      // For services
	IngressClass    string   `json:"ingress_class,omitempty"`     // For ingress
	StorageClass    string   `json:"storage_class,omitempty"`     // For PVCs
	VolumeMode      string   `json:"volume_mode,omitempty"`       // For PVs/PVCs
	AccessModes     []string `json:"access_modes,omitempty"`      // For PVs/PVCs
	
	// Pod-specific extended fields
	PodIP           string `json:"pod_ip,omitempty"`
	Phase           string `json:"phase,omitempty"`
	RestartCount    int    `json:"restart_count,omitempty"`
	QOSClass        string `json:"qos_class,omitempty"`
	
	// Tailpipe metadata (enriched automatically)
	SourceName      string     `json:"tp_source_name,omitempty"`
	SourceType      string     `json:"tp_source_type,omitempty"`
	CollectedAt     *time.Time `json:"tp_collected_at,omitempty"`
}

// OwnerReference represents an owner reference
type OwnerReference struct {
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller *bool  `json:"controller,omitempty"`
}

// ResourceReplicas represents replica counts for scalable resources
type ResourceReplicas struct {
	Desired     int32 `json:"desired"`
	Current     int32 `json:"current"`
	Ready       int32 `json:"ready"`
	Updated     int32 `json:"updated,omitempty"`
	Available   int32 `json:"available,omitempty"`
	Unavailable int32 `json:"unavailable,omitempty"`
}

// ResourceCondition represents a resource condition
type ResourceCondition struct {
	Type               string     `json:"type"`
	Status             string     `json:"status"`
	Reason             string     `json:"reason,omitempty"`
	Message            string     `json:"message,omitempty"`
	LastTransitionTime *time.Time `json:"last_transition_time,omitempty"`
}

// ResourceQueryFilters represents filters for resource queries
type ResourceQueryFilters struct {
	// Time range filters
	SinceTime  *time.Time
	UntilTime  *time.Time
	
	// Resource filters
	Namespaces      []string
	ResourceTypes   []string
	ResourceNames   []string
	Statuses        []string
	NodeNames       []string
	LabelSelector   labels.Selector
	
	// Content filters
	NamePattern     *regexp.Regexp
	
	// Pagination
	Limit           *int64
	
	// Include options
	IncludeOwnerRefs bool
	IncludeStatus    bool
	IncludeSpec      bool
}

// ResourceQueryOptions represents user-friendly query options
type ResourceQueryOptions struct {
	// Time range options
	SinceTime     *time.Time
	UntilTime     *time.Time
	SinceDuration string
	
	// Resource filtering
	Namespaces      []string
	ResourceTypes   []string  // Pod, Service, Deployment, etc.
	ResourceNames   []string
	Statuses        []string  // Running, Pending, Failed, etc.
	NodeNames       []string
	LabelSelectors  []string
	
	// Content filtering
	NamePattern     string  // regex pattern
	
	// Pagination and limits
	Limit           *int64
	
	// Include options for performance
	IncludeOwnerRefs bool
	IncludeStatus    bool
	IncludeSpec      bool
}

// ResourceCache represents cached resource data
type ResourceCache struct {
	resources       map[string]*ResourceRow  // key: namespace/resourceType/name
	lastUpdated     map[string]time.Time     // key: namespace/resourceType
	mu              sync.RWMutex
	cacheTTL        time.Duration
	maxCacheSize    int
	cacheHits       int64
	cacheMisses     int64
}

// ResourceType represents supported Kubernetes resource types
type ResourceType struct {
	Name         string
	APIVersion   string
	Namespaced   bool
	StatusField  string
	ListFunc     func(context.Context, internal.KubernetesClientInterface, internal.ListOptions) ([]runtime.Object, error)
}

// KubernetesResourcesTable implements the Table interface for cluster resources
type KubernetesResourcesTable struct {
	client        internal.KubernetesClientInterface
	config        *config.Config
	
	// Caching system for efficient resource management
	cache         *ResourceCache
	
	// Resource type registry
	resourceTypes map[string]ResourceType
	
	// Mutex for thread safety
	mu            sync.RWMutex
}

// Ensure the table implements the Table interface
var _ table.Table[*ResourceRow] = (*KubernetesResourcesTable)(nil)

// NewKubernetesResourcesTable creates a new resources table
func NewKubernetesResourcesTable() *KubernetesResourcesTable {
	t := &KubernetesResourcesTable{
		cache: &ResourceCache{
			resources:    make(map[string]*ResourceRow),
			lastUpdated:  make(map[string]time.Time),
			cacheTTL:     5 * time.Minute, // 5 minute cache TTL
			maxCacheSize: 10000,           // Max 10k resources in cache
		},
		resourceTypes: make(map[string]ResourceType),
	}
	
	// Initialize supported resource types
	t.initializeResourceTypes()
	
	return t
}

// Factory function for table registration
func NewKubernetesResourcesTableFactory() table.Table[*ResourceRow] {
	return NewKubernetesResourcesTable()
}

// Identifier returns the table name
func (t *KubernetesResourcesTable) Identifier() string {
	return "kubernetes_resources"
}

// GetSourceMetadata returns source metadata for the table
func (t *KubernetesResourcesTable) GetSourceMetadata() ([]*table.SourceMetadata[*ResourceRow], error) {
	return []*table.SourceMetadata[*ResourceRow]{
		{
			SourceName: "kubernetes_resources",
		},
	}, nil
}

// EnrichRow enriches a resource row with common Tailpipe fields
func (t *KubernetesResourcesTable) EnrichRow(row *ResourceRow, enrichment schema.SourceEnrichment) (*ResourceRow, error) {
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
func (t *KubernetesResourcesTable) CollectRows(ctx context.Context) (<-chan *ResourceRow, error) {
	if t.client == nil {
		if err := t.initializeClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
		}
	}
	
	// Create a channel for streaming rows
	rowChan := make(chan *ResourceRow, 100)
	
	// Start collection in a goroutine
	go func() {
		defer close(rowChan)
		
		// Collect resources and stream to channel
		resources, err := t.collectResources(ctx)
		if err != nil {
			slog.Error("Failed to collect resources", "error", err)
			return
		}
		
		for _, resource := range resources {
			select {
			case rowChan <- resource:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return rowChan, nil
}

// initializeResourceTypes initializes the registry of supported resource types
func (t *KubernetesResourcesTable) initializeResourceTypes() {
	t.resourceTypes = map[string]ResourceType{
		// Core resources
		"Pod": {
			Name:        "Pod",
			APIVersion:  "v1",
			Namespaced:  true,
			StatusField: "status.phase",
			ListFunc:    t.listPods,
		},
		"Service": {
			Name:        "Service",
			APIVersion:  "v1",
			Namespaced:  true,
			StatusField: "status",
			ListFunc:    t.listServices,
		},
		"ConfigMap": {
			Name:        "ConfigMap",
			APIVersion:  "v1",
			Namespaced:  true,
			StatusField: "metadata",
			ListFunc:    t.listConfigMaps,
		},
		"Secret": {
			Name:        "Secret",
			APIVersion:  "v1",
			Namespaced:  true,
			StatusField: "metadata",
			ListFunc:    t.listSecrets,
		},
		"PersistentVolume": {
			Name:        "PersistentVolume",
			APIVersion:  "v1",
			Namespaced:  false,
			StatusField: "status.phase",
			ListFunc:    t.listPersistentVolumes,
		},
		"PersistentVolumeClaim": {
			Name:        "PersistentVolumeClaim",
			APIVersion:  "v1",
			Namespaced:  true,
			StatusField: "status.phase",
			ListFunc:    t.listPersistentVolumeClaims,
		},
		// Apps resources
		"Deployment": {
			Name:        "Deployment",
			APIVersion:  "apps/v1",
			Namespaced:  true,
			StatusField: "status",
			ListFunc:    t.listDeployments,
		},
		"StatefulSet": {
			Name:        "StatefulSet",
			APIVersion:  "apps/v1",
			Namespaced:  true,
			StatusField: "status",
			ListFunc:    t.listStatefulSets,
		},
		"DaemonSet": {
			Name:        "DaemonSet",
			APIVersion:  "apps/v1",
			Namespaced:  true,
			StatusField: "status",
			ListFunc:    t.listDaemonSets,
		},
		// Networking resources
		"Ingress": {
			Name:        "Ingress",
			APIVersion:  "networking.k8s.io/v1",
			Namespaced:  true,
			StatusField: "status",
			ListFunc:    t.listIngresses,
		},
		"NetworkPolicy": {
			Name:        "NetworkPolicy",
			APIVersion:  "networking.k8s.io/v1",
			Namespaced:  true,
			StatusField: "metadata",
			ListFunc:    t.listNetworkPolicies,
		},
	}
}

// collectResources collects resources from Kubernetes cluster
func (t *KubernetesResourcesTable) collectResources(ctx context.Context) ([]*ResourceRow, error) {
	return t.collectResourcesWithFilters(ctx, ResourceQueryFilters{})
}

// collectResourcesWithFilters collects resources with filtering and caching
func (t *KubernetesResourcesTable) collectResourcesWithFilters(ctx context.Context, filters ResourceQueryFilters) ([]*ResourceRow, error) {
	if t.client == nil {
		if err := t.initializeClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", err)
		}
	}
	
	var allResources []*ResourceRow
	
	// Determine which resource types to collect
	resourceTypesToCollect := t.getResourceTypesToCollect(filters)
	
	// Collect each resource type
	for _, resourceType := range resourceTypesToCollect {
		resources, err := t.collectResourceType(ctx, resourceType, filters)
		if err != nil {
			slog.Warn("Failed to collect resource type", "type", resourceType, "error", err)
			continue
		}
		
		allResources = append(allResources, resources...)
	}
	
	// Apply post-collection filters
	allResources = t.applyPostCollectionFilters(allResources, filters)
	
	// Sort resources for consistent output
	t.sortResources(allResources)
	
	slog.Info("Collected resources", "total", len(allResources), "filters", t.logFiltersString(filters))
	
	return allResources, nil
}

// collectResourceType collects a specific resource type with caching
func (t *KubernetesResourcesTable) collectResourceType(ctx context.Context, resourceTypeName string, filters ResourceQueryFilters) ([]*ResourceRow, error) {
	resourceType, exists := t.resourceTypes[resourceTypeName]
	if !exists {
		return nil, fmt.Errorf("unsupported resource type: %s", resourceTypeName)
	}
	
	// Check cache first
	if cachedResources := t.getCachedResources(resourceTypeName, filters); cachedResources != nil {
		t.cache.mu.Lock()
		t.cache.cacheHits++
		t.cache.mu.Unlock()
		return cachedResources, nil
	}
	
	t.cache.mu.Lock()
	t.cache.cacheMisses++
	t.cache.mu.Unlock()
	
	// Build list options from filters
	listOpts := t.buildListOptions(filters)
	
	// Get raw resources using the specific list function
	rawResources, err := resourceType.ListFunc(ctx, t.client, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to list %s resources: %w", resourceTypeName, err)
	}
	
	slog.Debug("Raw resources found", "type", resourceTypeName, "count", len(rawResources))
	
	// Convert raw resources to ResourceRows
	var resources []*ResourceRow
	for _, rawResource := range rawResources {
		resourceRow, err := t.convertResourceToRowWithFilters(rawResource, filters)
		if err != nil {
			slog.Warn("Failed to convert resource to row", "type", resourceTypeName, "error", err)
			continue
		}
		
		if resourceRow != nil {
			resources = append(resources, resourceRow)
		}
	}
	
	// Cache the results
	t.cacheResources(resourceTypeName, resources, filters)
	
	return resources, nil
}

// convertResourceToRow converts a Kubernetes resource to a ResourceRow
func (t *KubernetesResourcesTable) convertResourceToRow(obj runtime.Object) (*ResourceRow, error) {
	return t.convertResourceToRowWithFilters(obj, ResourceQueryFilters{})
}

// convertResourceToRowWithFilters converts a resource with filtering
func (t *KubernetesResourcesTable) convertResourceToRowWithFilters(obj runtime.Object, filters ResourceQueryFilters) (*ResourceRow, error) {
	// Convert to unstructured to access common fields
	objBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal object: %w", err)
	}
	
	var objMap map[string]interface{}
	if err := json.Unmarshal(objBytes, &objMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal object: %w", err)
	}
	
	// Extract metadata
	metadata, ok := objMap["metadata"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("object has no metadata")
	}
	
	// Early filtering based on basic criteria
	if !t.passesBasicFilters(objMap, metadata, filters) {
		return nil, nil // Filtered out
	}
	
	row := &ResourceRow{
		ClusterName: t.config.GetClusterName(),
	}
	
	// Set basic metadata
	if name, ok := metadata["name"].(string); ok {
		row.ResourceName = name
	}
	
	if namespace, ok := metadata["namespace"].(string); ok {
		row.Namespace = namespace
	}
	
	if uid, ok := metadata["uid"].(string); ok {
		row.UID = uid
	}
	
	if resourceVersion, ok := metadata["resourceVersion"].(string); ok {
		row.ResourceVersion = resourceVersion
	}
	
	// Set creation timestamp
	if creationTimestamp, ok := metadata["creationTimestamp"].(string); ok {
		if timestamp, err := time.Parse(time.RFC3339, creationTimestamp); err == nil {
			row.CreatedAt = &timestamp
		}
	}
	
	// Set labels and annotations
	if labels, ok := metadata["labels"].(map[string]interface{}); ok {
		row.Labels = labels
	} else {
		row.Labels = make(map[string]interface{})
	}
	
	if annotations, ok := metadata["annotations"].(map[string]interface{}); ok {
		row.Annotations = annotations
	} else {
		row.Annotations = make(map[string]interface{})
	}
	
	// Set spec (only if requested for performance)
	if filters.IncludeSpec {
		if spec, ok := objMap["spec"].(map[string]interface{}); ok {
			row.Spec = spec
		} else {
			row.Spec = make(map[string]interface{})
		}
	}
	
	// Set owner references (only if requested)
	if filters.IncludeOwnerRefs {
		row.OwnerReferences = t.extractOwnerReferences(metadata)
	}
	
	// Determine resource type and set specific fields
	kind, _ := objMap["kind"].(string)
	row.ResourceType = kind
	
	// Set resource-specific fields based on type
	switch kind {
	case "Pod":
		t.setPodSpecificFields(row, objMap, filters)
	case "Service":
		t.setServiceSpecificFields(row, objMap, filters)
	case "Deployment":
		t.setDeploymentSpecificFields(row, objMap, filters)
	case "StatefulSet":
		t.setStatefulSetSpecificFields(row, objMap, filters)
	case "DaemonSet":
		t.setDaemonSetSpecificFields(row, objMap, filters)
	case "PersistentVolume":
		t.setPersistentVolumeSpecificFields(row, objMap, filters)
	case "PersistentVolumeClaim":
		t.setPersistentVolumeClaimSpecificFields(row, objMap, filters)
	case "Ingress":
		t.setIngressSpecificFields(row, objMap, filters)
	case "ConfigMap", "Secret":
		t.setConfigMapSecretSpecificFields(row, objMap, filters)
	case "NetworkPolicy":
		t.setNetworkPolicySpecificFields(row, objMap, filters)
	default:
		// Set generic status
		row.Status = t.extractGenericStatus(objMap)
	}
	
	// Apply final content filters
	if !t.passesContentFilters(row, filters) {
		return nil, nil // Filtered out
	}
	
	return row, nil
}

// Resource-specific field setters

// setPodSpecificFields sets pod-specific fields
func (t *KubernetesResourcesTable) setPodSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	// Set spec fields
	if spec, ok := objMap["spec"].(map[string]interface{}); ok {
		if nodeName, ok := spec["nodeName"].(string); ok {
			row.NodeName = nodeName
		}
	}
	
	// Set status fields
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		if phase, ok := status["phase"].(string); ok {
			row.Phase = phase
			row.Status = phase
		}
		
		if podIP, ok := status["podIP"].(string); ok {
			row.PodIP = podIP
		}
		
		if qosClass, ok := status["qosClass"].(string); ok {
			row.QOSClass = qosClass
		}
		
		// Calculate total restart count
		if containerStatuses, ok := status["containerStatuses"].([]interface{}); ok {
			totalRestarts := 0
			for _, cs := range containerStatuses {
				if containerStatus, ok := cs.(map[string]interface{}); ok {
					if restarts, ok := containerStatus["restartCount"].(float64); ok {
						totalRestarts += int(restarts)
					}
				}
			}
			row.RestartCount = totalRestarts
		}
		
		// Extract conditions if requested
		if filters.IncludeStatus {
			row.Conditions = t.extractConditions(status)
			row.StatusDetails = status
		}
	}
}

// setServiceSpecificFields sets service-specific fields
func (t *KubernetesResourcesTable) setServiceSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	// Set spec fields
	if spec, ok := objMap["spec"].(map[string]interface{}); ok {
		if serviceType, ok := spec["type"].(string); ok {
			row.ServiceType = serviceType
		}
		
		if clusterIP, ok := spec["clusterIP"].(string); ok {
			row.ClusterIP = clusterIP
		}
		
		if externalIPs, ok := spec["externalIPs"].([]interface{}); ok {
			for _, ip := range externalIPs {
				if ipStr, ok := ip.(string); ok {
					row.ExternalIPs = append(row.ExternalIPs, ipStr)
				}
			}
		}
	}
	
	// Services are generally "Active" if they exist
	row.Status = "Active"
	if filters.IncludeStatus {
		if status, ok := objMap["status"].(map[string]interface{}); ok {
			row.StatusDetails = status
		}
	}
}

// setDeploymentSpecificFields sets deployment-specific fields
func (t *KubernetesResourcesTable) setDeploymentSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		replicas := &ResourceReplicas{}
		
		if replicas.Desired, ok = t.getInt32FromInterface(status["replicas"]); !ok {
			if spec, specOk := objMap["spec"].(map[string]interface{}); specOk {
				replicas.Desired, _ = t.getInt32FromInterface(spec["replicas"])
			}
		}
		
		replicas.Current, _ = t.getInt32FromInterface(status["replicas"])
		replicas.Ready, _ = t.getInt32FromInterface(status["readyReplicas"])
		replicas.Updated, _ = t.getInt32FromInterface(status["updatedReplicas"])
		replicas.Available, _ = t.getInt32FromInterface(status["availableReplicas"])
		replicas.Unavailable, _ = t.getInt32FromInterface(status["unavailableReplicas"])
		
		row.Replicas = replicas
		
		// Determine status based on deployment conditions
		row.Status = t.getDeploymentStatus(status)
		
		if filters.IncludeStatus {
			row.Conditions = t.extractConditions(status)
			row.StatusDetails = status
		}
	}
}

// setStatefulSetSpecificFields sets statefulset-specific fields
func (t *KubernetesResourcesTable) setStatefulSetSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		replicas := &ResourceReplicas{}
		
		if spec, specOk := objMap["spec"].(map[string]interface{}); specOk {
			replicas.Desired, _ = t.getInt32FromInterface(spec["replicas"])
		}
		
		replicas.Current, _ = t.getInt32FromInterface(status["replicas"])
		replicas.Ready, _ = t.getInt32FromInterface(status["readyReplicas"])
		replicas.Updated, _ = t.getInt32FromInterface(status["updatedReplicas"])
		
		row.Replicas = replicas
		row.Status = t.getStatefulSetStatus(status)
		
		if filters.IncludeStatus {
			row.Conditions = t.extractConditions(status)
			row.StatusDetails = status
		}
	}
}

// setDaemonSetSpecificFields sets daemonset-specific fields
func (t *KubernetesResourcesTable) setDaemonSetSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		replicas := &ResourceReplicas{}
		
		replicas.Desired, _ = t.getInt32FromInterface(status["desiredNumberScheduled"])
		replicas.Current, _ = t.getInt32FromInterface(status["currentNumberScheduled"])
		replicas.Ready, _ = t.getInt32FromInterface(status["numberReady"])
		replicas.Updated, _ = t.getInt32FromInterface(status["updatedNumberScheduled"])
		replicas.Available, _ = t.getInt32FromInterface(status["numberAvailable"])
		replicas.Unavailable, _ = t.getInt32FromInterface(status["numberUnavailable"])
		
		row.Replicas = replicas
		row.Status = t.getDaemonSetStatus(status)
		
		if filters.IncludeStatus {
			row.Conditions = t.extractConditions(status)
			row.StatusDetails = status
		}
	}
}

// setPersistentVolumeSpecificFields sets PV-specific fields
func (t *KubernetesResourcesTable) setPersistentVolumeSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	if spec, ok := objMap["spec"].(map[string]interface{}); ok {
		if storageClass, ok := spec["storageClassName"].(string); ok {
			row.StorageClass = storageClass
		}
		
		if volumeMode, ok := spec["volumeMode"].(string); ok {
			row.VolumeMode = volumeMode
		}
		
		if accessModes, ok := spec["accessModes"].([]interface{}); ok {
			for _, mode := range accessModes {
				if modeStr, ok := mode.(string); ok {
					row.AccessModes = append(row.AccessModes, modeStr)
				}
			}
		}
	}
	
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		if phase, ok := status["phase"].(string); ok {
			row.Status = phase
		}
		
		if filters.IncludeStatus {
			row.StatusDetails = status
		}
	}
}

// setPersistentVolumeClaimSpecificFields sets PVC-specific fields
func (t *KubernetesResourcesTable) setPersistentVolumeClaimSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	if spec, ok := objMap["spec"].(map[string]interface{}); ok {
		if storageClass, ok := spec["storageClassName"].(string); ok {
			row.StorageClass = storageClass
		}
		
		if volumeMode, ok := spec["volumeMode"].(string); ok {
			row.VolumeMode = volumeMode
		}
		
		if accessModes, ok := spec["accessModes"].([]interface{}); ok {
			for _, mode := range accessModes {
				if modeStr, ok := mode.(string); ok {
					row.AccessModes = append(row.AccessModes, modeStr)
				}
			}
		}
	}
	
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		if phase, ok := status["phase"].(string); ok {
			row.Status = phase
		}
		
		if filters.IncludeStatus {
			row.StatusDetails = status
		}
	}
}

// setIngressSpecificFields sets ingress-specific fields
func (t *KubernetesResourcesTable) setIngressSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	if spec, ok := objMap["spec"].(map[string]interface{}); ok {
		if ingressClass, ok := spec["ingressClassName"].(string); ok {
			row.IngressClass = ingressClass
		}
	}
	
	// Ingress status is typically based on load balancer status
	row.Status = "Active"
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		if loadBalancer, ok := status["loadBalancer"].(map[string]interface{}); ok {
			if ingress, ok := loadBalancer["ingress"].([]interface{}); ok && len(ingress) > 0 {
				row.Status = "Ready"
			}
		}
		
		if filters.IncludeStatus {
			row.StatusDetails = status
		}
	}
}

// setConfigMapSecretSpecificFields sets ConfigMap/Secret-specific fields
func (t *KubernetesResourcesTable) setConfigMapSecretSpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	// ConfigMaps and Secrets are active if they exist
	row.Status = "Active"
	
	// For secrets, we only include metadata (not data for security)
	if filters.IncludeStatus {
		if row.ResourceType == "Secret" {
			// Only include type and metadata count for secrets
			statusDetails := make(map[string]interface{})
			if spec, ok := objMap["data"].(map[string]interface{}); ok {
				statusDetails["data_keys_count"] = len(spec)
			}
			if secretType, ok := objMap["type"].(string); ok {
				statusDetails["type"] = secretType
			}
			row.StatusDetails = statusDetails
		} else {
			// For ConfigMaps, include data key count
			statusDetails := make(map[string]interface{})
			if data, ok := objMap["data"].(map[string]interface{}); ok {
				statusDetails["data_keys_count"] = len(data)
			}
			row.StatusDetails = statusDetails
		}
	}
}

// setNetworkPolicySpecificFields sets NetworkPolicy-specific fields
func (t *KubernetesResourcesTable) setNetworkPolicySpecificFields(row *ResourceRow, objMap map[string]interface{}, filters ResourceQueryFilters) {
	// NetworkPolicies are active if they exist
	row.Status = "Active"
	
	if filters.IncludeStatus {
		if status, ok := objMap["status"].(map[string]interface{}); ok {
			row.StatusDetails = status
		}
	}
}

// SetClient sets the Kubernetes client and config
func (t *KubernetesResourcesTable) SetClient(client internal.KubernetesClientInterface, config *config.Config) {
	t.client = client
	t.config = config
}

// initializeClient initializes with default configuration (fallback)
func (t *KubernetesResourcesTable) initializeClient() error {
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

// Resource list functions for each supported type

// listPods lists pods using the client
func (t *KubernetesResourcesTable) listPods(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "pods", opts)
}

// listServices lists services
func (t *KubernetesResourcesTable) listServices(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "services", opts)
}

// listDeployments lists deployments
func (t *KubernetesResourcesTable) listDeployments(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "deployments", opts)
}

// listStatefulSets lists statefulsets
func (t *KubernetesResourcesTable) listStatefulSets(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "statefulsets", opts)
}

// listDaemonSets lists daemonsets
func (t *KubernetesResourcesTable) listDaemonSets(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "daemonsets", opts)
}

// listConfigMaps lists configmaps
func (t *KubernetesResourcesTable) listConfigMaps(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "configmaps", opts)
}

// listSecrets lists secrets
func (t *KubernetesResourcesTable) listSecrets(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "secrets", opts)
}

// listPersistentVolumes lists persistent volumes
func (t *KubernetesResourcesTable) listPersistentVolumes(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "persistentvolumes", opts)
}

// listPersistentVolumeClaims lists persistent volume claims
func (t *KubernetesResourcesTable) listPersistentVolumeClaims(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "persistentvolumeclaims", opts)
}

// listIngresses lists ingresses
func (t *KubernetesResourcesTable) listIngresses(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "ingresses", opts)
}

// listNetworkPolicies lists network policies
func (t *KubernetesResourcesTable) listNetworkPolicies(ctx context.Context, client internal.KubernetesClientInterface, opts internal.ListOptions) ([]runtime.Object, error) {
	return client.ListResources(ctx, "networkpolicies", opts)
}

// Helper methods for resource processing

// extractOwnerReferences extracts owner references from metadata
func (t *KubernetesResourcesTable) extractOwnerReferences(metadata map[string]interface{}) []OwnerReference {
	var ownerRefs []OwnerReference
	
	if ownerReferences, ok := metadata["ownerReferences"].([]interface{}); ok {
		for _, ref := range ownerReferences {
			if refMap, ok := ref.(map[string]interface{}); ok {
				ownerRef := OwnerReference{}
				
				if apiVersion, ok := refMap["apiVersion"].(string); ok {
					ownerRef.APIVersion = apiVersion
				}
				if kind, ok := refMap["kind"].(string); ok {
					ownerRef.Kind = kind
				}
				if name, ok := refMap["name"].(string); ok {
					ownerRef.Name = name
				}
				if uid, ok := refMap["uid"].(string); ok {
					ownerRef.UID = uid
				}
				if controller, ok := refMap["controller"].(bool); ok {
					ownerRef.Controller = &controller
				}
				
				ownerRefs = append(ownerRefs, ownerRef)
			}
		}
	}
	
	return ownerRefs
}

// extractConditions extracts conditions from status
func (t *KubernetesResourcesTable) extractConditions(status map[string]interface{}) []ResourceCondition {
	var conditions []ResourceCondition
	
	if conditionsInterface, ok := status["conditions"].([]interface{}); ok {
		for _, cond := range conditionsInterface {
			if condMap, ok := cond.(map[string]interface{}); ok {
				condition := ResourceCondition{}
				
				if condType, ok := condMap["type"].(string); ok {
					condition.Type = condType
				}
				if condStatus, ok := condMap["status"].(string); ok {
					condition.Status = condStatus
				}
				if reason, ok := condMap["reason"].(string); ok {
					condition.Reason = reason
				}
				if message, ok := condMap["message"].(string); ok {
					condition.Message = message
				}
				if lastTransitionTime, ok := condMap["lastTransitionTime"].(string); ok {
					if timestamp, err := time.Parse(time.RFC3339, lastTransitionTime); err == nil {
						condition.LastTransitionTime = &timestamp
					}
				}
				
				conditions = append(conditions, condition)
			}
		}
	}
	
	return conditions
}

// getInt32FromInterface safely converts interface{} to int32
func (t *KubernetesResourcesTable) getInt32FromInterface(val interface{}) (int32, bool) {
	switch v := val.(type) {
	case int32:
		return v, true
	case int:
		return int32(v), true
	case float64:
		return int32(v), true
	case float32:
		return int32(v), true
	default:
		return 0, false
	}
}

// Status determination methods for different resource types

// getDeploymentStatus determines deployment status based on conditions
func (t *KubernetesResourcesTable) getDeploymentStatus(status map[string]interface{}) string {
	conditions := t.extractConditions(status)
	
	for _, condition := range conditions {
		if condition.Type == "Available" && condition.Status == "True" {
			return "Ready"
		}
		if condition.Type == "Progressing" && condition.Status == "False" {
			return "Failed"
		}
	}
	
	// Check replica status
	if replicas, ok := t.getInt32FromInterface(status["replicas"]); ok {
		if readyReplicas, ok := t.getInt32FromInterface(status["readyReplicas"]); ok {
			if readyReplicas == 0 {
				return "Pending"
			} else if readyReplicas < replicas {
				return "Updating"
			} else {
				return "Ready"
			}
		}
	}
	
	return "Unknown"
}

// getStatefulSetStatus determines statefulset status
func (t *KubernetesResourcesTable) getStatefulSetStatus(status map[string]interface{}) string {
	if replicas, ok := t.getInt32FromInterface(status["replicas"]); ok {
		if readyReplicas, ok := t.getInt32FromInterface(status["readyReplicas"]); ok {
			if readyReplicas == 0 {
				return "Pending"
			} else if readyReplicas < replicas {
				return "Updating"
			} else {
				return "Ready"
			}
		}
	}
	return "Unknown"
}

// getDaemonSetStatus determines daemonset status
func (t *KubernetesResourcesTable) getDaemonSetStatus(status map[string]interface{}) string {
	if desired, ok := t.getInt32FromInterface(status["desiredNumberScheduled"]); ok {
		if ready, ok := t.getInt32FromInterface(status["numberReady"]); ok {
			if ready == 0 {
				return "Pending"
			} else if ready < desired {
				return "Updating"
			} else {
				return "Ready"
			}
		}
	}
	return "Unknown"
}

// extractGenericStatus extracts status from any resource type
func (t *KubernetesResourcesTable) extractGenericStatus(objMap map[string]interface{}) string {
	if status, ok := objMap["status"].(map[string]interface{}); ok {
		if phase, ok := status["phase"].(string); ok {
			return phase
		}
		if state, ok := status["state"].(string); ok {
			return state
		}
	}
	return "Active"
}

// Caching methods for efficient resource management

// getCachedResources retrieves resources from cache if valid
func (t *KubernetesResourcesTable) getCachedResources(resourceType string, filters ResourceQueryFilters) []*ResourceRow {
	t.cache.mu.RLock()
	defer t.cache.mu.RUnlock()
	
	// Check if cache is still valid
	cacheKey := t.getCacheKey(resourceType, filters)
	if lastUpdated, exists := t.cache.lastUpdated[cacheKey]; exists {
		if time.Since(lastUpdated) < t.cache.cacheTTL {
			// Return cached resources that match filters
			var results []*ResourceRow
			for key, resource := range t.cache.resources {
				if strings.HasPrefix(key, resourceType) {
					results = append(results, resource)
				}
			}
			return results
		}
	}
	
	return nil
}

// cacheResources stores resources in cache
func (t *KubernetesResourcesTable) cacheResources(resourceType string, resources []*ResourceRow, filters ResourceQueryFilters) {
	t.cache.mu.Lock()
	defer t.cache.mu.Unlock()
	
	// Evict old entries if cache is too large
	if len(t.cache.resources) > t.cache.maxCacheSize {
		t.evictOldEntries()
	}
	
	// Store resources in cache
	for _, resource := range resources {
		key := fmt.Sprintf("%s/%s/%s", resource.ResourceType, resource.Namespace, resource.ResourceName)
		t.cache.resources[key] = resource
	}
	
	// Update cache timestamp
	cacheKey := t.getCacheKey(resourceType, filters)
	t.cache.lastUpdated[cacheKey] = time.Now()
}

// evictOldEntries removes old cache entries
func (t *KubernetesResourcesTable) evictOldEntries() {
	// Remove 25% of oldest entries
	entriesToRemove := len(t.cache.resources) / 4
	if entriesToRemove == 0 {
		entriesToRemove = 1
	}
	
	// Sort by last updated time and remove oldest
	type cacheEntry struct {
		key         string
		lastUpdated time.Time
	}
	
	var entries []cacheEntry
	for key, lastUpdated := range t.cache.lastUpdated {
		entries = append(entries, cacheEntry{key, lastUpdated})
	}
	
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUpdated.Before(entries[j].lastUpdated)
	})
	
	for i := 0; i < entriesToRemove && i < len(entries); i++ {
		delete(t.cache.lastUpdated, entries[i].key)
		// Remove associated resource entries
		for resourceKey := range t.cache.resources {
			if strings.HasPrefix(resourceKey, entries[i].key) {
				delete(t.cache.resources, resourceKey)
			}
		}
	}
}

// getCacheKey generates a cache key for filters
func (t *KubernetesResourcesTable) getCacheKey(resourceType string, filters ResourceQueryFilters) string {
	hash := md5.New()
	hash.Write([]byte(fmt.Sprintf("%s:%v:%v:%v", resourceType, filters.Namespaces, filters.LabelSelector, filters.Statuses)))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// Filtering and query methods

// getResourceTypesToCollect determines which resource types to collect based on filters
func (t *KubernetesResourcesTable) getResourceTypesToCollect(filters ResourceQueryFilters) []string {
	if len(filters.ResourceTypes) > 0 {
		return filters.ResourceTypes
	}
	
	// Default to all supported resource types
	var types []string
	for resourceType := range t.resourceTypes {
		types = append(types, resourceType)
	}
	return types
}

// buildListOptions builds Kubernetes list options from query filters
func (t *KubernetesResourcesTable) buildListOptions(filters ResourceQueryFilters) internal.ListOptions {
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

// passesBasicFilters checks if a resource passes basic filters
func (t *KubernetesResourcesTable) passesBasicFilters(objMap, metadata map[string]interface{}, filters ResourceQueryFilters) bool {
	// Time range filtering
	if filters.SinceTime != nil || filters.UntilTime != nil {
		if creationTimestamp, ok := metadata["creationTimestamp"].(string); ok {
			if timestamp, err := time.Parse(time.RFC3339, creationTimestamp); err == nil {
				if filters.SinceTime != nil && timestamp.Before(*filters.SinceTime) {
					return false
				}
				if filters.UntilTime != nil && timestamp.After(*filters.UntilTime) {
					return false
				}
			}
		}
	}
	
	// Namespace filtering (if multiple namespaces)
	if len(filters.Namespaces) > 0 {
		if namespace, ok := metadata["namespace"].(string); ok {
			if !t.matchesNamespaceFilter(namespace, filters.Namespaces) {
				return false
			}
		}
	}
	
	// Resource name filtering
	if len(filters.ResourceNames) > 0 {
		if name, ok := metadata["name"].(string); ok {
			if !t.containsString(filters.ResourceNames, name) {
				return false
			}
		}
	}
	
	return true
}

// passesContentFilters checks if a resource row passes content filters
func (t *KubernetesResourcesTable) passesContentFilters(row *ResourceRow, filters ResourceQueryFilters) bool {
	// Status filtering
	if len(filters.Statuses) > 0 && !t.containsString(filters.Statuses, row.Status) {
		return false
	}
	
	// Node name filtering
	if len(filters.NodeNames) > 0 && row.NodeName != "" && !t.containsString(filters.NodeNames, row.NodeName) {
		return false
	}
	
	// Name pattern filtering
	if filters.NamePattern != nil && !filters.NamePattern.MatchString(row.ResourceName) {
		return false
	}
	
	return true
}

// applyPostCollectionFilters applies filters that couldn't be applied during collection
func (t *KubernetesResourcesTable) applyPostCollectionFilters(resources []*ResourceRow, filters ResourceQueryFilters) []*ResourceRow {
	var filtered []*ResourceRow
	
	for _, resource := range resources {
		// All filtering is already applied during conversion
		filtered = append(filtered, resource)
	}
	
	// Apply final limit
	if filters.Limit != nil && int64(len(filtered)) > *filters.Limit {
		filtered = filtered[:*filters.Limit]
	}
	
	return filtered
}

// sortResources sorts resources for consistent output
func (t *KubernetesResourcesTable) sortResources(resources []*ResourceRow) {
	sort.Slice(resources, func(i, j int) bool {
		// Sort by namespace, then resource type, then name
		if resources[i].Namespace != resources[j].Namespace {
			return resources[i].Namespace < resources[j].Namespace
		}
		if resources[i].ResourceType != resources[j].ResourceType {
			return resources[i].ResourceType < resources[j].ResourceType
		}
		return resources[i].ResourceName < resources[j].ResourceName
	})
}

// Utility methods

// matchesNamespaceFilter checks if a namespace matches the filter
func (t *KubernetesResourcesTable) matchesNamespaceFilter(namespace string, allowedNamespaces []string) bool {
	for _, allowed := range allowedNamespaces {
		if allowed == "*" || allowed == namespace {
			return true
		}
	}
	return false
}

// containsString checks if a slice contains a string
func (t *KubernetesResourcesTable) containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// logFiltersString creates a string representation of filters for logging
func (t *KubernetesResourcesTable) logFiltersString(filters ResourceQueryFilters) string {
	var parts []string
	
	if len(filters.Namespaces) > 0 {
		parts = append(parts, fmt.Sprintf("namespaces=%v", filters.Namespaces))
	}
	if len(filters.ResourceTypes) > 0 {
		parts = append(parts, fmt.Sprintf("resource_types=%v", filters.ResourceTypes))
	}
	if len(filters.Statuses) > 0 {
		parts = append(parts, fmt.Sprintf("statuses=%v", filters.Statuses))
	}
	if filters.SinceTime != nil {
		parts = append(parts, fmt.Sprintf("since=%s", filters.SinceTime.Format(time.RFC3339)))
	}
	if filters.UntilTime != nil {
		parts = append(parts, fmt.Sprintf("until=%s", filters.UntilTime.Format(time.RFC3339)))
	}
	
	return strings.Join(parts, ", ")
}

// Query interface methods

// QueryResourcesWithOptions provides an interface for querying resources with options
func (t *KubernetesResourcesTable) QueryResourcesWithOptions(ctx context.Context, options ResourceQueryOptions) ([]*ResourceRow, error) {
	filters := t.convertOptionsToFilters(options)
	return t.collectResourcesWithFilters(ctx, filters)
}

// convertOptionsToFilters converts ResourceQueryOptions to ResourceQueryFilters
func (t *KubernetesResourcesTable) convertOptionsToFilters(options ResourceQueryOptions) ResourceQueryFilters {
	filters := ResourceQueryFilters{
		SinceTime:        options.SinceTime,
		UntilTime:        options.UntilTime,
		Namespaces:       options.Namespaces,
		ResourceTypes:    options.ResourceTypes,
		ResourceNames:    options.ResourceNames,
		Statuses:         options.Statuses,
		NodeNames:        options.NodeNames,
		Limit:           options.Limit,
		IncludeOwnerRefs: options.IncludeOwnerRefs,
		IncludeStatus:    options.IncludeStatus,
		IncludeSpec:      options.IncludeSpec,
	}
	
	// Parse since duration if provided
	if options.SinceDuration != "" && options.SinceTime == nil {
		if duration, err := time.ParseDuration(options.SinceDuration); err == nil {
			since := time.Now().Add(-duration)
			filters.SinceTime = &since
		}
	}
	
	// Parse name pattern
	if options.NamePattern != "" {
		if regex, err := regexp.Compile(options.NamePattern); err == nil {
			filters.NamePattern = regex
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

// GetCacheStats returns cache statistics
func (t *KubernetesResourcesTable) GetCacheStats() map[string]interface{} {
	t.cache.mu.RLock()
	defer t.cache.mu.RUnlock()
	
	return map[string]interface{}{
		"cache_size":    len(t.cache.resources),
		"cache_hits":    t.cache.cacheHits,
		"cache_misses":  t.cache.cacheMisses,
		"cache_ttl":     t.cache.cacheTTL.String(),
		"max_size":      t.cache.maxCacheSize,
	}
}

// Close cleans up resources
func (t *KubernetesResourcesTable) Close() error {
	// Clear cache
	if t.cache != nil {
		t.cache.mu.Lock()
		t.cache.resources = make(map[string]*ResourceRow)
		t.cache.lastUpdated = make(map[string]time.Time)
		t.cache.mu.Unlock()
	}
	
	if t.client != nil {
		return t.client.Close()
	}
	return nil
}
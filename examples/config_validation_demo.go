package examples

import (
	"fmt"

	"github.com/jlgore/tailpipe-plugin-kubernetes/config"
)

func ConfigValidationDemo() {
	fmt.Println("Tailpipe Kubernetes Plugin - Configuration Validation Demo")
	fmt.Println("=========================================================")

	// Test 1: Default configuration
	fmt.Println("\n1. Default Configuration:")
	defaultConfig := &config.Config{}
	defaultConfig.ApplyDefaults()

	if err := defaultConfig.Validate(); err != nil {
		fmt.Printf("   ❌ Validation failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Valid configuration\n")
		fmt.Printf("   %s\n", defaultConfig.String())
	}

	// Test 2: Production-like configuration
	fmt.Println("\n2. Production Configuration:")
	prodConfig := &config.Config{
		KubeconfigPath:    stringPtr("~/.kube/config"),
		KubeconfigContext: stringPtr("production"),
		Namespace:         stringPtr("default,kube-system,monitoring"),
		FollowLogs:        boolPtr(true),
		SinceTime:         stringPtr("2h"),
		MaxLogRequests:    intPtr(15),
		IncludeEvents:     boolPtr(true),
		ClusterName:       stringPtr("prod-cluster"),
	}

	if err := prodConfig.Validate(); err != nil {
		fmt.Printf("   ❌ Validation failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Valid configuration\n")
		fmt.Printf("   %s\n", prodConfig.String())
	}

	// Test 3: Service account configuration
	fmt.Println("\n3. Service Account Configuration:")
	saConfig := &config.Config{
		ServiceAccount: boolPtr(true),
		Namespace:      stringPtr("monitoring"),
		FollowLogs:     boolPtr(true),
		SinceTime:      stringPtr("0s"),
	}

	if err := saConfig.Validate(); err != nil {
		fmt.Printf("   ❌ Validation failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Valid configuration\n")
		fmt.Printf("   %s\n", saConfig.String())
	}

	// Test 4: Invalid configurations
	fmt.Println("\n4. Invalid Configuration Examples:")

	invalidConfigs := []struct {
		name   string
		config *config.Config
	}{
		{
			name: "Invalid Server URL",
			config: &config.Config{
				ServerURL: stringPtr("invalid-url"),
			},
		},
		{
			name: "Invalid Max Log Requests",
			config: &config.Config{
				MaxLogRequests: intPtr(0),
			},
		},
		{
			name: "Invalid Since Time",
			config: &config.Config{
				SinceTime: stringPtr("invalid-time"),
			},
		},
		{
			name: "Invalid Namespace",
			config: &config.Config{
				Namespace: stringPtr("Invalid_Namespace"),
			},
		},
	}

	for _, test := range invalidConfigs {
		fmt.Printf("\n   %s:\n", test.name)
		if err := test.config.Validate(); err != nil {
			fmt.Printf("   ❌ Expected validation error: %v\n", err)
		} else {
			fmt.Printf("   ⚠️  Unexpectedly valid!\n")
		}
	}

	// Test 5: Namespace list parsing
	fmt.Println("\n5. Namespace List Parsing:")
	namespaceTests := []string{
		"*",
		"default",
		"default,kube-system,monitoring",
		"default, kube-system , monitoring",
	}

	for _, ns := range namespaceTests {
		testConfig := &config.Config{Namespace: &ns}
		nsList := testConfig.GetNamespaceList()
		fmt.Printf("   '%s' → %v\n", ns, nsList)
	}

	// Test 6: Configuration getter methods
	fmt.Println("\n6. Configuration Getter Methods:")
	fullConfig := &config.Config{
		KubeconfigPath:    stringPtr("~/.kube/test-config"),
		KubeconfigContext: stringPtr("test-context"),
		ServiceAccount:    boolPtr(false),
		ServerURL:         stringPtr("https://k8s.example.com"),
		Namespace:         stringPtr("test-namespace"),
		FollowLogs:        boolPtr(true),
		SinceTime:         stringPtr("30m"),
		MaxLogRequests:    intPtr(20),
		IncludeEvents:     boolPtr(false),
		IncludeNodeLogs:   boolPtr(true),
		ClusterName:       stringPtr("test-cluster"),
	}

	fmt.Printf("   Kubeconfig Path: %s\n", fullConfig.GetKubeconfigPath())
	fmt.Printf("   Context: %s\n", fullConfig.GetKubeconfigContext())
	fmt.Printf("   Service Account: %t\n", fullConfig.GetServiceAccount())
	fmt.Printf("   Server URL: %s\n", fullConfig.GetServerURL())
	fmt.Printf("   Namespace: %s\n", fullConfig.GetNamespace())
	fmt.Printf("   Follow Logs: %t\n", fullConfig.GetFollowLogs())
	fmt.Printf("   Since Time: %s\n", fullConfig.GetSinceTime())
	fmt.Printf("   Max Log Requests: %d\n", fullConfig.GetMaxLogRequests())
	fmt.Printf("   Include Events: %t\n", fullConfig.GetIncludeEvents())
	fmt.Printf("   Include Node Logs: %t\n", fullConfig.GetIncludeNodeLogs())
	fmt.Printf("   Cluster Name: %s\n", fullConfig.GetClusterName())
}

// Helper functions for creating pointers
func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}

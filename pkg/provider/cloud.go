package provider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rpc "kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer"
)

const (
	// ProviderName is the name of the kubevirt provider
	ProviderName = "kubevirt"
)

var scheme = runtime.NewScheme()

func init() {
	cloudprovider.RegisterCloudProvider(ProviderName, kubevirtCloudProviderFactory)
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := kubevirtv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
}

type Cloud struct {
	namespace    string
	client       client.Client
	tenantClient client.Client
	config       CloudConfig
}

type CloudConfig struct {
	Kubeconfig   string             `yaml:"kubeconfig"`
	LoadBalancer LoadBalancerConfig `yaml:"loadBalancer"`
	InstancesV2  InstancesV2Config  `yaml:"instancesV2"`
	Namespace    string             `yaml:"namespace"`
	InfraLabels  map[string]string  `yaml:"infraLabels"`
}

type LoadBalancerConfig struct {
	// Enabled activates the load balancer interface of the CCM
	Enabled bool `yaml:"enabled"`

	// CreationPollInterval determines how many seconds to wait for the load balancer creation between retries
	CreationPollInterval *int `yaml:"creationPollInterval,omitempty"`

	// CreationPollTimeout determines how many seconds to wait for the load balancer creation
	CreationPollTimeout *int `yaml:"creationPollTimeout,omitempty"`

	// Selectorless delegate endpointslices creation on third party by
	// skipping service selector creation
	Selectorless *bool `yaml:"selectorless,omitempty"`

	// EnableEPSController determines if the EPS controller is enabled
	// This is a temporary flag to enable/disable the EPS controller
	// When disabled the service selector is used.
	EnableEPSController *bool `yaml:"enableEPSController,omitempty"`

	// RPCServerAddr is the address of the RPC server for load balancer management
	RPCServerAddr string `yaml:"rpcServerAddr,omitempty"`

	// RPCKeepAlive is the keep-alive period in seconds for RPC connections
	RPCKeepAlive *int `yaml:"rpcKeepAlive,omitempty"`

	// RPCRetryMax is the maximum number of retry attempts for RPC calls
	RPCRetryMax *int `yaml:"rpcRetryMax,omitempty"`

	// NetworkID is the network ID used for load balancer
	NetworkID string `yaml:"networkID,omitempty"`

	// SubnetID is the subnet ID used for load balancer
	SubnetID string `yaml:"subnetID,omitempty"`

	// TenantID is the OpenStack tenant ID for load balancer
	TenantID string `yaml:"tenantID,omitempty"`

	// FipNetworkID is the external network ID for floating IP allocation
	FipNetworkID string `yaml:"fipNetworkID,omitempty"`

	// IpType controls the type of IP address requested for the load balancer.
	// Valid values: "external" (default), "internal", "both".
	//   - "external": FIP is allocated, status uses floating IP.
	//   - "internal": No FIP, status uses the load balancer's internal IP.
	//   - "both": FIP is allocated for status, internal IP stored as annotation.
	IpType string `yaml:"ipType,omitempty"`

	// OnlyServiceController when true disables node/route controllers and reads
	// network/subnet/tenant config from service annotations instead of global config.
	OnlyServiceController bool `yaml:"onlyServiceController,omitempty"`

	// ApiKey is the API key for authenticating with the RPC server.
	// When empty, authentication is disabled.
	ApiKey string `yaml:"apiKey,omitempty"`
}

type InstancesV2Config struct {
	// Enabled activates the instances interface of the CCM
	Enabled bool `yaml:"enabled"`
	// ZoneAndRegionEnabled indicates if need to get Region and zone labels from the cloud provider
	ZoneAndRegionEnabled bool `yaml:"zoneAndRegionEnabled"`
}

// createDefaultCloudConfig creates a CloudConfig object filled with default values.
// These default values should be overwritten by values read from the cloud-config file.
func createDefaultCloudConfig() CloudConfig {
	return CloudConfig{
		LoadBalancer: LoadBalancerConfig{
			Enabled:              true,
			CreationPollInterval: pointer.Int(DefaultLoadBalancerCreatePollInterval),
			CreationPollTimeout:  pointer.Int(DefaultLoadBalancerCreatePollTimeout),
		},
		InstancesV2: InstancesV2Config{
			Enabled:              true,
			ZoneAndRegionEnabled: true,
		},
	}
}

func NewCloudConfigFromBytes(configBytes []byte) (CloudConfig, error) {
	var config = createDefaultCloudConfig()
	err := yaml.Unmarshal(configBytes, &config)
	if err != nil {
		return CloudConfig{}, err
	}
	return config, nil
}

func kubevirtCloudProviderFactory(config io.Reader) (cloudprovider.Interface, error) {
	if config == nil {
		return nil, fmt.Errorf("No %s cloud provider config file given", ProviderName)
	}

	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to read cloud provider config: %v", err)
	}
	cloudConf, err := NewCloudConfigFromBytes(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal cloud provider config: %v", err)
	}
	namespace := cloudConf.Namespace
	var restConfig *rest.Config
	if cloudConf.Kubeconfig == "" {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	} else {
		var infraKubeConfig string
		infraKubeConfig, err = GetInfraKubeConfig(cloudConf.Kubeconfig)
		if err != nil {
			return nil, err
		}
		var clientConfig clientcmd.ClientConfig
		clientConfig, err = clientcmd.NewClientConfigFromBytes([]byte(infraKubeConfig))
		if err != nil {
			return nil, err
		}
		restConfig, err = clientConfig.ClientConfig()
		if err != nil {
			return nil, err
		}
		if namespace == "" {
			namespace, _, err = clientConfig.Namespace()
			if err != nil {
				klog.Errorf("Could not find namespace in client config: %v", err)
				return nil, err
			}
		}
	}
	c, err := client.New(restConfig, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}
	return &Cloud{
		namespace: namespace,
		client:    c,
		config:    cloudConf,
	}, nil
}

// Initialize provides the Cloud with a kubernetes client builder and may spawn goroutines
// to perform housekeeping activities within the Cloud provider.
func (c *Cloud) Initialize(clientBuilder cloudprovider.ControllerClientBuilder, stop <-chan struct{}) {
	tenantConfig := clientBuilder.ConfigOrDie("")
	tenantClient, err := client.New(tenantConfig, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		klog.Fatalf("Failed to create tenant client: %v", err)
	}
	c.tenantClient = tenantClient
}

// LoadBalancer returns a balancer interface. Also returns true if the interface is supported, false otherwise.
func (c *Cloud) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	if !c.config.LoadBalancer.Enabled {
		return nil, false
	}

	var rpcClient *rpc.Client
	if c.config.LoadBalancer.RPCServerAddr != "" {
		rpcConfig := &rpc.Config{
			ServerAddr: c.config.LoadBalancer.RPCServerAddr,
			Timeout:    30 * time.Second,
			RetryMax:   3,
			RetryDelay: 100 * time.Millisecond,
			ApiKey:     c.config.LoadBalancer.ApiKey,
			DialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		}
		if c.config.LoadBalancer.RPCKeepAlive != nil {
			rpcConfig.Timeout = time.Duration(*c.config.LoadBalancer.RPCKeepAlive) * time.Second
		}
		if c.config.LoadBalancer.RPCRetryMax != nil {
			rpcConfig.RetryMax = *c.config.LoadBalancer.RPCRetryMax
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := rpc.NewClient(ctx, rpcConfig)
		if err != nil {
			klog.Errorf("Failed to create RPC client: %v", err)
		} else {
			rpcClient = client
		}
	}

	return &loadbalancer{
		namespace:    c.namespace,
		client:       c.client,
		tenantClient: c.tenantClient,
		config:       c.config.LoadBalancer,
		rpcClient:    rpcClient,
		retryCounts:  make(map[string]int),
	}, true
}

// Instances returns an instances interface. Also returns true if the interface is supported, false otherwise.
func (c *Cloud) Instances() (cloudprovider.Instances, bool) {
	return nil, false
}

func (c *Cloud) InstancesV2() (cloudprovider.InstancesV2, bool) {
	if !c.config.InstancesV2.Enabled {
		return nil, false
	}
	return &instancesV2{
		namespace: c.namespace,
		client:    c.client,
		config:    &c.config.InstancesV2,
	}, true
}

// Zones returns a zones interface. Also returns true if the interface is supported, false otherwise.
// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
func (c *Cloud) Zones() (cloudprovider.Zones, bool) {
	return nil, false
}

// Clusters returns a clusters interface.  Also returns true if the interface is supported, false otherwise.
func (c *Cloud) Clusters() (cloudprovider.Clusters, bool) {
	return nil, false
}

// Routes returns a routes interface along with whether the interface is supported.
func (c *Cloud) Routes() (cloudprovider.Routes, bool) {
	return nil, false
}

// ProviderName returns the Cloud provider ID.
func (c *Cloud) ProviderName() string {
	return ProviderName
}

// HasClusterID returns true if a ClusterID is required and set
func (c *Cloud) HasClusterID() bool {
	return true
}

func (c *Cloud) GetInfraKubeconfig() (string, error) {
	return GetInfraKubeConfig(c.config.Kubeconfig)
}

func (c *Cloud) Namespace() string {
	return c.namespace
}

func (c *Cloud) GetCloudConfig() CloudConfig {
	return c.config
}

func GetInfraKubeConfig(infraKubeConfigPath string) (string, error) {
	config, err := os.Open(infraKubeConfigPath)
	if err != nil {
		return "", fmt.Errorf("Couldn't open infra-kubeconfig: %v", err)
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(config)
	if err != nil {
		return "", fmt.Errorf("Failed to read infra-kubeconfig: %v", err)
	}
	return buf.String(), nil
}

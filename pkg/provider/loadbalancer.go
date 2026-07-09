package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rpc "kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer"
	loadbalancerv1 "kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer/gen"
)

const (
	DefaultLoadBalancerCreatePollInterval = 5
	DefaultLoadBalancerCreatePollTimeout  = 300

	TenantServiceNameLabelKey        = "cluster.x-k8s.io/tenant-service-name"
	TenantServiceNamespaceLabelKey   = "cluster.x-k8s.io/tenant-service-namespace"
	TenantClusterNameLabelKey        = "cluster.x-k8s.io/cluster-name"
	TenantNodeRoleLabelKey           = "cluster.x-k8s.io/role"
	LoadBalancerIDAnnotationKey           = "kubevirt.io/loadbalancer-id"
	LoadBalancerCreateCountAnnotationKey  = "kubevirt.io/loadbalancer-create-count"
	LoadBalancerListenerHashAnnotationKey = "kubevirt.io/loadbalancer-listener-hash"
	LoadBalancerInternalIPAnnotationKey   = "kubevirt.io/loadbalancer-internal-ip"
	HTTPRoutePathAnnotationKey            = "kubevirt.io/http-path"
	HTTPRouteMethodAnnotationKey          = "kubevirt.io/http-method"
	LoadBalancerFinalizer                 = "service.kubernetes.io/load-balancer-cleanup"
	LoadBalancerPendingHostname           = "pending"

	maxLoadBalancerRetries        = 10
	maxLoadBalancerCreateCount    = 3

	defaultHealthMonitorInterval  = int32(10)
	defaultHealthMonitorTimeout   = int32(5)
	defaultHealthMonitorUnhealthy = int32(3)
	defaultHealthMonitorHealthy   = int32(2)
	defaultHealthCheckPath        = "/"
	defaultClientConnLimit        = int32(0)
	defaultAlgorithm              = loadbalancerv1.Algorithm_ALGORITHM_ROUND_ROBIN
)

type rpcLBClient interface {
	CreateLoadBalancer(ctx context.Context, req *loadbalancerv1.CreateLoadBalancerRequest) (*loadbalancerv1.CreateLoadBalancerResponse, error)
	GetLoadBalancer(ctx context.Context, req *loadbalancerv1.GetLoadBalancerRequest) (*loadbalancerv1.GetLoadBalancerResponse, error)
	UpdateLoadBalancer(ctx context.Context, req *loadbalancerv1.UpdateLoadBalancerRequest) (*loadbalancerv1.UpdateLoadBalancerResponse, error)
	DeleteLoadBalancer(ctx context.Context, req *loadbalancerv1.DeleteLoadBalancerRequest) (*loadbalancerv1.DeleteLoadBalancerResponse, error)
	ListLoadBalancers(ctx context.Context, req *loadbalancerv1.ListLoadBalancersRequest) (*loadbalancerv1.ListLoadBalancersResponse, error)
}

type loadbalancer struct {
	namespace    string
	client       client.Client
	tenantClient client.Client
	config       LoadBalancerConfig
	rpcClient    rpcLBClient

	mu          sync.Mutex
	retryCounts map[string]int
}

// GetLoadBalancer returns whether the specified load balancer exists, and
// if so, what its status is.
func (lb *loadbalancer) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (status *corev1.LoadBalancerStatus, exists bool, err error) {
	if lb.rpcClient == nil {
		return nil, false, fmt.Errorf("load balancer RPC client is not configured")
	}

	lbID := service.Annotations[LoadBalancerIDAnnotationKey]
	if lbID == "" {
		return nil, false, nil
	}

	resp, err := lb.rpcClient.GetLoadBalancer(ctx, &loadbalancerv1.GetLoadBalancerRequest{Id: lbID})
	if err != nil {
		klog.Errorf("Failed to get load balancer from RPC: %v", err)
		return nil, false, stdErrors.New(rpc.ToRPCError(err))
	}
	if resp.GetLoadBalancer() != nil {
		return lb.loadBalancerStatusFromResponse(resp.GetLoadBalancer()), true, nil
	}

	return nil, false, nil
}

// GetLoadBalancerName is an implementation of LoadBalancer.GetLoadBalancerName.
func (lb *loadbalancer) GetLoadBalancerName(ctx context.Context, clusterName string, service *corev1.Service) string {
	return cloudprovider.DefaultLoadBalancerName(service)
}

func (lb *loadbalancer) pendingStatus() *corev1.LoadBalancerStatus {
	return &corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{{Hostname: LoadBalancerPendingHostname}},
	}
}

func (lb *loadbalancer) pollLoadBalancerReady(ctx context.Context, lbID string) (*loadbalancerv1.LoadBalancer, error) {
	interval := time.Duration(DefaultLoadBalancerCreatePollInterval) * time.Second
	timeout := time.Duration(DefaultLoadBalancerCreatePollTimeout) * time.Second
	if lb.config.CreationPollInterval != nil {
		interval = time.Duration(*lb.config.CreationPollInterval) * time.Second
	}
	if lb.config.CreationPollTimeout != nil {
		timeout = time.Duration(*lb.config.CreationPollTimeout) * time.Second
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := lb.rpcClient.GetLoadBalancer(ctx, &loadbalancerv1.GetLoadBalancerRequest{Id: lbID})
		if err != nil {
			klog.Warningf("Failed to get load balancer status (will retry): %v", err)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(interval):
			}
			continue
		}

		lbStatus := resp.GetLoadBalancer()
		if lbStatus != nil && lbStatus.State == loadbalancerv1.State_STATE_READY && lbStatus.Ip != "" {
			if !lb.isFipEnabled() || lbStatus.FipState == loadbalancerv1.FipState_FIP_STATE_ACTIVE {
				return lbStatus, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}

	return nil, fmt.Errorf("load balancer %s not ready within %v", lbID, timeout)
}

// EnsureLoadBalancer creates a new load balancer 'name', or updates the existing one. Returns the status of the balancer
func (lb *loadbalancer) EnsureLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) (*corev1.LoadBalancerStatus, error) {
	if lb.rpcClient == nil {
		return nil, fmt.Errorf("load balancer RPC client is not configured")
	}

	if service.DeletionTimestamp != nil {
		if err := lb.EnsureLoadBalancerDeleted(ctx, clusterName, service); err != nil {
			klog.Errorf("Failed to cleanup load balancer during deletion: %v", err)
		}
		return nil, nil
	}

	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)
	lbID := service.Annotations[LoadBalancerIDAnnotationKey]

	listeners := lb.buildListeners(ctx, service, nodes, clusterName)
	if lbID == "" {
		if lb.isFipEnabled() && lb.config.FipNetworkID == "" {
			return nil, fmt.Errorf("FIP network ID is not configured; load balancer creation requires floating IP support")
		}

		tenantID := lb.config.TenantID
		if tenantID == "" {
			tenantID = service.Namespace
		}

		spec := &loadbalancerv1.LoadBalancerSpec{
			TenantId:              tenantID,
			NetworkId:             lb.config.NetworkID,
			SubnetId:              lb.config.SubnetID,
			Listeners:             listeners,
			ClientConnLimit:       defaultClientConnLimit,
			SecurityGroupDisabled: true,
			QosPolicyDisabled:     true,
		}

		createReq := &loadbalancerv1.CreateLoadBalancerRequest{
			Name:           lbName,
			Spec:           spec,
			IdempotencyKey: string(service.UID),
		}

		if lb.isFipEnabled() && lb.config.FipNetworkID != "" {
			createReq.Fip = pointer.String("")
			createReq.FipNetworkId = &lb.config.FipNetworkID
		}

		resp, err := lb.rpcClient.CreateLoadBalancer(ctx, createReq)
		if err != nil {
			klog.Errorf("Failed to create load balancer via RPC: %v", err)
			return nil, stdErrors.New(rpc.ToRPCError(err))
		}

		if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerIDAnnotationKey, resp.Id); err != nil {
			klog.Errorf("Failed to update service with LB ID: %v", err)
			return nil, err
		}

		listenersHash := listenersHashStr(listeners)
		lb.ensureServiceAnnotation(ctx, service, LoadBalancerCreateCountAnnotationKey, "1")
		lb.ensureServiceAnnotation(ctx, service, LoadBalancerListenerHashAnnotationKey, listenersHash)

		lb.clearRetryCount(string(service.UID))

		lbStatus, err := lb.pollLoadBalancerReady(ctx, resp.Id)
		if err != nil {
			return nil, err
		}
		lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
		return lb.loadBalancerStatusFromResponse(lbStatus), nil
	}

	getResp, err := lb.rpcClient.GetLoadBalancer(ctx, &loadbalancerv1.GetLoadBalancerRequest{Id: lbID})
	if err != nil {
		return nil, stdErrors.New(rpc.ToRPCError(err))
	}

	lbStatus := getResp.GetLoadBalancer()
	if lbStatus != nil && lbStatus.State == loadbalancerv1.State_STATE_READY && lbStatus.Ip != "" {
		if lb.configChanged(service, listeners) {
			updateReq := &loadbalancerv1.UpdateLoadBalancerRequest{
				Id:                   lbID,
				Listeners:            listeners,
				SecurityGroupDisabled: pointer.Bool(true),
				QosPolicyDisabled:     pointer.Bool(true),
			}
			if _, err := lb.rpcClient.UpdateLoadBalancer(ctx, updateReq); err != nil {
				klog.Errorf("Failed to update load balancer via RPC: %v", err)
				return nil, stdErrors.New(rpc.ToRPCError(err))
			}

			lb.ensureServiceAnnotation(ctx, service, LoadBalancerListenerHashAnnotationKey, listenersHashStr(listeners))

			lbStatus, err := lb.pollLoadBalancerReady(ctx, lbID)
			if err != nil {
				return nil, err
			}
			lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
			return lb.loadBalancerStatusFromResponse(lbStatus), nil
		}

		if lb.isFipEnabled() && lbStatus.FipState != loadbalancerv1.FipState_FIP_STATE_ACTIVE {
			lbStatus, err := lb.pollLoadBalancerReady(ctx, lbID)
			if err != nil {
				return nil, err
			}
			lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
			return lb.loadBalancerStatusFromResponse(lbStatus), nil
		}

		lb.clearRetryCount(string(service.UID))

		return lb.loadBalancerStatusFromResponse(lbStatus), nil
	}

	lb.mu.Lock()
	retries := lb.retryCounts[string(service.UID)]
	lb.mu.Unlock()

	if retries >= maxLoadBalancerRetries {
		createCount := lb.getCreateCount(service)
		if createCount >= maxLoadBalancerCreateCount {
			return nil, fmt.Errorf("load balancer %s failed to provision after %d create attempts", lbID, maxLoadBalancerCreateCount)
		}

		klog.Warningf("Load balancer %s not ready after %d retries, recreating (attempt %d)", lbID, maxLoadBalancerRetries, createCount+1)

		if lb.isFipEnabled() && lb.config.FipNetworkID == "" {
			return nil, fmt.Errorf("FIP network ID is not configured; load balancer recreation requires floating IP support")
		}

		lb.rpcClient.DeleteLoadBalancer(ctx, &loadbalancerv1.DeleteLoadBalancerRequest{Id: lbID})

		tenantID := lb.config.TenantID
		if tenantID == "" {
			tenantID = service.Namespace
		}

		createReq := &loadbalancerv1.CreateLoadBalancerRequest{
			Name: lbName,
			Spec: &loadbalancerv1.LoadBalancerSpec{
				TenantId:              tenantID,
				NetworkId:             lb.config.NetworkID,
				SubnetId:              lb.config.SubnetID,
				Listeners:             listeners,
				ClientConnLimit:       defaultClientConnLimit,
				SecurityGroupDisabled: true,
				QosPolicyDisabled:     true,
			},
			IdempotencyKey: string(service.UID),
		}

		if lb.isFipEnabled() && lb.config.FipNetworkID != "" {
			createReq.Fip = pointer.String("")
			createReq.FipNetworkId = &lb.config.FipNetworkID
		}

		createResp, createErr := lb.rpcClient.CreateLoadBalancer(ctx, createReq)
		if createErr != nil {
			return nil, stdErrors.New(rpc.ToRPCError(createErr))
		}

		if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerIDAnnotationKey, createResp.Id); err != nil {
			klog.Errorf("Failed to update service with new LB ID: %v", err)
			return nil, err
		}
		lb.ensureServiceAnnotation(ctx, service, LoadBalancerCreateCountAnnotationKey, strconv.Itoa(createCount+1))
		lb.ensureServiceAnnotation(ctx, service, LoadBalancerListenerHashAnnotationKey, listenersHashStr(listeners))

		lb.clearRetryCount(string(service.UID))

		lbStatus, err := lb.pollLoadBalancerReady(ctx, createResp.Id)
		if err != nil {
			return nil, err
		}
		lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
		return lb.loadBalancerStatusFromResponse(lbStatus), nil
	}

	if lb.configChanged(service, listeners) {
		updateReq := &loadbalancerv1.UpdateLoadBalancerRequest{
			Id:                   lbID,
			Listeners:            listeners,
			SecurityGroupDisabled: pointer.Bool(true),
			QosPolicyDisabled:     pointer.Bool(true),
		}
		if _, err := lb.rpcClient.UpdateLoadBalancer(ctx, updateReq); err != nil {
			klog.Errorf("Failed to update load balancer via RPC: %v", err)
			return nil, stdErrors.New(rpc.ToRPCError(err))
		}

		lb.ensureServiceAnnotation(ctx, service, LoadBalancerListenerHashAnnotationKey, listenersHashStr(listeners))

		lbStatus, err := lb.pollLoadBalancerReady(ctx, lbID)
		if err != nil {
			return nil, err
		}
		lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
		return lb.loadBalancerStatusFromResponse(lbStatus), nil
	}

	lb.mu.Lock()
	lb.retryCounts[string(service.UID)] = retries + 1
	lb.mu.Unlock()

	return lb.pendingStatus(), nil
}

func (lb *loadbalancer) getCreateCount(service *corev1.Service) int {
	if countStr, ok := service.Annotations[LoadBalancerCreateCountAnnotationKey]; ok && countStr != "" {
		if n, err := strconv.Atoi(countStr); err == nil {
			return n
		}
	}
	return 0
}

func (lb *loadbalancer) clearRetryCount(uid string) {
	lb.mu.Lock()
	delete(lb.retryCounts, uid)
	lb.mu.Unlock()
}

func (lb *loadbalancer) isFipEnabled() bool {
	ipType := strings.ToLower(lb.config.IpType)
	return ipType == "" || ipType == "external" || ipType == "both"
}

func (lb *loadbalancer) isInternalMode() bool {
	return strings.ToLower(lb.config.IpType) == "internal"
}

func (lb *loadbalancer) isBothMode() bool {
	return strings.ToLower(lb.config.IpType) == "both"
}

func (lb *loadbalancer) storeInternalIpIfBothMode(ctx context.Context, service *corev1.Service, lbStatus *loadbalancerv1.LoadBalancer) {
	if lb.isBothMode() && lbStatus != nil && lbStatus.Ip != "" {
		if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerInternalIPAnnotationKey, lbStatus.Ip); err != nil {
			klog.Errorf("Failed to store internal IP annotation: %v", err)
		}
	}
}

func listenersHashStr(listeners []*loadbalancerv1.Listener) string {
	data, err := json.Marshal(listeners)
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (lb *loadbalancer) configChanged(service *corev1.Service, listeners []*loadbalancerv1.Listener) bool {
	return service.Annotations[LoadBalancerListenerHashAnnotationKey] != listenersHashStr(listeners)
}

func (lb *loadbalancer) loadBalancerStatusFromResponse(lbStatus *loadbalancerv1.LoadBalancer) *corev1.LoadBalancerStatus {
	if lbStatus == nil {
		return nil
	}
	ip := lbStatus.Ip
	if lb.isFipEnabled() && lbStatus.Fip != "" {
		ip = lbStatus.Fip
	}
	if ip == "" {
		return nil
	}
	return &corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{{IP: ip}},
	}
}

func (lb *loadbalancer) buildListeners(ctx context.Context, service *corev1.Service, nodes []*corev1.Node, clusterName string) []*loadbalancerv1.Listener {
	listeners := make([]*loadbalancerv1.Listener, 0, len(service.Spec.Ports))

	for _, port := range service.Spec.Ports {
		protocol := loadbalancerv1.Protocol_PROTOCOL_TCP
		if _, hasHTTPRoute := service.Annotations[HTTPRoutePathAnnotationKey]; hasHTTPRoute {
			protocol = loadbalancerv1.Protocol_PROTOCOL_HTTP
		}

		listener := &loadbalancerv1.Listener{
			Port:        port.Port,
			Protocol:    protocol,
			Algorithm:   defaultAlgorithm,
			TlsEnabled:  false,
			BackendRefs: lb.buildBackendRefs(ctx, port, nodes, clusterName),
		}

		if protocol == loadbalancerv1.Protocol_PROTOCOL_HTTP {
			healthCheckPath := defaultHealthCheckPath
			healthCheckMethod := loadbalancerv1.HttpMethod_HTTP_METHOD_GET
			if path, ok := service.Annotations[HTTPRoutePathAnnotationKey]; ok && path != "" {
				healthCheckPath = path
				if listener.HttpRoute == nil {
					listener.HttpRoute = &loadbalancerv1.HttpRoute{}
				}
				listener.HttpRoute.Path = path
				listener.HttpRoute.PathType = loadbalancerv1.HttpPathType_HTTP_PATH_TYPE_PREFIX
			}
			if method, ok := service.Annotations[HTTPRouteMethodAnnotationKey]; ok && method != "" {
				healthCheckMethod = lb.parseHTTPMethod(method)
				if listener.HttpRoute == nil {
					listener.HttpRoute = &loadbalancerv1.HttpRoute{}
				}
				listener.HttpRoute.Method = lb.parseHTTPMethod(method)
			}
			listener.HealthMonitor = &loadbalancerv1.HealthMonitor{
				Interval:              defaultHealthMonitorInterval,
				Timeout:               defaultHealthMonitorTimeout,
				UnhealthyThreshold:    defaultHealthMonitorUnhealthy,
				HealthyThreshold:      defaultHealthMonitorHealthy,
				HttpHealthCheckPath:   healthCheckPath,
				HttpHealthCheckMethod: healthCheckMethod,
				ExpectedStatusCodes:   []int32{200},
			}
		} else {
			listener.HealthMonitor = &loadbalancerv1.HealthMonitor{
				Interval:           defaultHealthMonitorInterval,
				Timeout:            defaultHealthMonitorTimeout,
				UnhealthyThreshold: defaultHealthMonitorUnhealthy,
				HealthyThreshold:   defaultHealthMonitorHealthy,
			}
		}

		listeners = append(listeners, listener)
	}

	return listeners
}

func (lb *loadbalancer) parseHTTPMethod(method string) loadbalancerv1.HttpMethod {
	switch strings.ToUpper(method) {
	case "GET":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_GET
	case "POST":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_POST
	case "PUT":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_PUT
	case "DELETE":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_DELETE
	case "PATCH":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_PATCH
	case "HEAD":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_HEAD
	case "OPTIONS":
		return loadbalancerv1.HttpMethod_HTTP_METHOD_OPTIONS
	default:
		return loadbalancerv1.HttpMethod_HTTP_METHOD_GET
	}
}

func (lb *loadbalancer) buildBackendRefs(ctx context.Context, port corev1.ServicePort, nodes []*corev1.Node, clusterName string) []*loadbalancerv1.BackendRef {
	backendRefs := make([]*loadbalancerv1.BackendRef, 0)

	if len(nodes) > 0 {
		for _, node := range nodes {
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeInternalIP {
					backendRefs = append(backendRefs, &loadbalancerv1.BackendRef{
						Ip:   addr.Address,
						Port: port.NodePort,
					})
				}
			}
		}

		if len(backendRefs) == 0 {
			klog.Warningf("No internal IPs found for backend refs, trying external IPs")
			for _, node := range nodes {
				for _, addr := range node.Status.Addresses {
					if addr.Type == corev1.NodeExternalIP {
						backendRefs = append(backendRefs, &loadbalancerv1.BackendRef{
							Ip:   addr.Address,
							Port: port.NodePort,
						})
					}
				}
			}
		}
	}

	if len(backendRefs) == 0 && clusterName != "" {
		klog.Warningf("No backend IPs from nodes, falling back to VMI IPs for cluster %s", clusterName)
		backendRefs = lb.buildBackendRefsFromVMI(ctx, port, clusterName)
	}

	return backendRefs
}

func (lb *loadbalancer) buildBackendRefsFromVMI(ctx context.Context, port corev1.ServicePort, clusterName string) []*loadbalancerv1.BackendRef {
	backendRefs := make([]*loadbalancerv1.BackendRef, 0)

	vmiLabels := client.MatchingLabels{
		TenantNodeRoleLabelKey:    "worker",
		TenantClusterNameLabelKey: clusterName,
	}

	var vmList kubevirtv1.VirtualMachineInstanceList
	if err := lb.client.List(ctx, &vmList, client.InNamespace(lb.namespace), vmiLabels); err != nil {
		klog.Errorf("Failed to list VMIs for backend refs: %v", err)
		return backendRefs
	}

	for _, vmi := range vmList.Items {
		for _, iface := range vmi.Status.Interfaces {
			if iface.IP != "" {
				backendRefs = append(backendRefs, &loadbalancerv1.BackendRef{
					Ip:   iface.IP,
					Port: port.NodePort,
				})
				break
			}
		}
	}

	if len(backendRefs) == 0 {
		klog.Warningf("No VMI IPs found for cluster %s", clusterName)
	}

	return backendRefs
}

// UpdateLoadBalancer updates hosts under the specified load balancer.
func (lb *loadbalancer) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	if lb.rpcClient == nil {
		return fmt.Errorf("load balancer RPC client is not configured")
	}

	lbID := service.Annotations[LoadBalancerIDAnnotationKey]
	if lbID == "" {
		return fmt.Errorf("load balancer ID not found in service annotations")
	}

	listeners := lb.buildListeners(ctx, service, nodes, clusterName)
	_, err := lb.rpcClient.UpdateLoadBalancer(ctx, &loadbalancerv1.UpdateLoadBalancerRequest{
		Id:                   lbID,
		Listeners:            listeners,
		SecurityGroupDisabled: pointer.Bool(true),
		QosPolicyDisabled:     pointer.Bool(true),
	})
	if err != nil {
		klog.Errorf("Failed to update load balancer via RPC: %v", err)
		return stdErrors.New(rpc.ToRPCError(err))
	}

	return nil
}

// EnsureLoadBalancerDeleted deletes the specified load balancer if it exists.
func (lb *loadbalancer) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)
	lbID := service.Annotations[LoadBalancerIDAnnotationKey]

	if lbID == "" && lb.rpcClient != nil {
		resp, listErr := lb.rpcClient.ListLoadBalancers(ctx, &loadbalancerv1.ListLoadBalancersRequest{
			TenantId: lb.config.TenantID,
		})
		if listErr == nil {
			for _, summary := range resp.GetLoadBalancers() {
				if summary.Name == lbName {
					lbID = summary.Id
					klog.Infof("Found load balancer by name %s with ID %s", lbName, lbID)
					break
				}
			}
		}
	}

	if lbID != "" && lb.rpcClient != nil {
		_, err := lb.rpcClient.DeleteLoadBalancer(ctx, &loadbalancerv1.DeleteLoadBalancerRequest{Id: lbID})
		if err != nil {
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.NotFound {
				klog.Errorf("Failed to delete load balancer via RPC: %v", err)
				return stdErrors.New(rpc.ToRPCError(err))
			}
			klog.Infof("Load balancer %s already deleted", lbID)
		}
	}

	return nil
}

func (lb *loadbalancer) ensureServiceAnnotation(ctx context.Context, service *corev1.Service, key, value string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest corev1.Service
		if err := lb.tenantClient.Get(ctx, client.ObjectKey{Name: service.Name, Namespace: service.Namespace}, &latest); err != nil {
			return err
		}
		if latest.Annotations != nil && latest.Annotations[key] == value {
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[key] = value
		return lb.tenantClient.Update(ctx, &latest)
	})
}





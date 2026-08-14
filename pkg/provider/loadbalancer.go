package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
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

	TenantServiceNameLabelKey             = "cluster.x-k8s.io/tenant-service-name"
	TenantServiceNamespaceLabelKey        = "cluster.x-k8s.io/tenant-service-namespace"
	TenantClusterNameLabelKey             = "cluster.x-k8s.io/cluster-name"
	TenantNodeRoleLabelKey                = "cluster.x-k8s.io/role"
	LoadBalancerIDAnnotationKey           = "kubevirt.io/loadbalancer-id"
	LoadBalancerCreateCountAnnotationKey  = "kubevirt.io/loadbalancer-create-count"
	LoadBalancerListenerHashAnnotationKey = "kubevirt.io/loadbalancer-listener-hash"
	LoadBalancerInternalIPAnnotationKey   = "kubevirt.io/loadbalancer-internal-ip"
	ServiceNetworkIDAnnotationKey         = "loadbalancer.kubevirt.io/network-id"
	ServiceSubnetIDAnnotationKey          = "loadbalancer.kubevirt.io/subnet-id"
	ServiceTenantIDAnnotationKey          = "loadbalancer.kubevirt.io/tenant-id"
	ServiceIpTypeAnnotationKey            = "loadbalancer.kubevirt.io/ip-type"
	HTTPRoutePathAnnotationKey            = "kubevirt.io/http-path"
	HTTPRouteMethodAnnotationKey          = "kubevirt.io/http-method"
	LoadBalancerFinalizer                 = "service.kubernetes.io/load-balancer-cleanup"
	LoadBalancerPendingHostname           = "pending"

	maxLoadBalancerRetries     = 10
	maxLoadBalancerCreateCount = 3

	defaultHealthMonitorInterval  = int32(10)
	defaultHealthMonitorTimeout   = int32(5)
	defaultHealthMonitorUnhealthy = int32(3)
	defaultHealthMonitorHealthy   = int32(2)
	defaultHealthCheckPath        = "/"
	defaultClientConnLimit        = int32(0)
	defaultAlgorithm              = loadbalancerv1.Algorithm_ALGORITHM_ROUND_ROBIN

	// Every listener this CCM builds carries exactly one rule with exactly one
	// backend, so the backend needs no distinguishing name. The weight has to be
	// set explicitly: the API's rule-weight-sum rule rejects a rule whose
	// backends all have weight 0, and it is evaluated before the server-side
	// "unset means 1" default can apply.
	defaultBackendName   = "default"
	defaultBackendWeight = int32(1)

	// The API caps a page at 200 and rejects anything below 1.
	listLoadBalancersPageSize = int32(200)
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

	if lb.shouldSkipService(service) {
		return nil, false, nil
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
		return lb.loadBalancerStatusFromResponse(service, resp.GetLoadBalancer()), true, nil
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

func (lb *loadbalancer) pollLoadBalancerReady(ctx context.Context, service *corev1.Service, lbID string) (*loadbalancerv1.LoadBalancer, error) {
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
		if lbStatus != nil {
			// A failed load balancer never recovers on its own; waiting out the
			// remaining timeout only delays the recreate path and hides the
			// reason the server reported.
			if lbStatus.State == loadbalancerv1.State_STATE_FAILED {
				return nil, fmt.Errorf("load balancer %s failed%s", lbID, lbErrorDetail(lbStatus))
			}
			if lbStatus.State == loadbalancerv1.State_STATE_READY && lbStatus.Ip != "" {
				if !lb.isFipEnabled(service) || lbStatus.FipState == loadbalancerv1.FipState_FIP_STATE_ACTIVE {
					return lbStatus, nil
				}
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

	if lb.shouldSkipService(service) {
		return nil, cloudprovider.ImplementedElsewhere
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
	if err := validateListeners(listeners); err != nil {
		return nil, err
	}

	if lbID == "" {
		if lb.isFipEnabled(service) && lb.config.FipNetworkID == "" {
			return nil, fmt.Errorf("FIP network ID is not configured; load balancer creation requires floating IP support")
		}

		tenantID := lb.getTenantID(service)

		spec := &loadbalancerv1.LoadBalancerSpec{
			TenantId:              tenantID,
			NetworkId:             lb.getNetworkID(service),
			SubnetId:              lb.getSubnetID(service),
			Listeners:             listeners,
			ClientConnLimit:       defaultClientConnLimit,
			SecurityGroupDisabled: true,
			QosPolicyDisabled:     true,
		}

		createReq := &loadbalancerv1.CreateLoadBalancerRequest{
			Name:           lbName,
			Spec:           spec,
			IdempotencyKey: createIdempotencyKey(service, lb.getCreateCount(service)),
		}

		if lb.isFipEnabled(service) && lb.config.FipNetworkID != "" {
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

		if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerCreateCountAnnotationKey, "1"); err != nil {
			klog.Errorf("Failed to record create count on service %s/%s: %v", service.Namespace, service.Name, err)
		}
		lb.recordListenerHash(ctx, service, listeners)

		lb.clearRetryCount(string(service.UID))

		lbStatus, err := lb.pollLoadBalancerReady(ctx, service, resp.Id)
		if err != nil {
			return nil, err
		}
		lb.updateKamajiAdvertiseAddress(ctx, service, lbStatus.Ip)
		lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
		return lb.loadBalancerStatusFromResponse(service, lbStatus), nil
	}

	getResp, err := lb.rpcClient.GetLoadBalancer(ctx, &loadbalancerv1.GetLoadBalancerRequest{Id: lbID})
	if err != nil {
		return nil, stdErrors.New(rpc.ToRPCError(err))
	}

	lbStatus := getResp.GetLoadBalancer()
	if lbStatus != nil && lbStatus.State == loadbalancerv1.State_STATE_READY && lbStatus.Ip != "" {
		if lb.configChanged(service, listeners) {
			updateReq := &loadbalancerv1.UpdateLoadBalancerRequest{
				Id:                    lbID,
				Listeners:             mergeListenerIDs(listeners, lbStatus.GetSpec().GetListeners()),
				SecurityGroupDisabled: pointer.Bool(true),
				QosPolicyDisabled:     pointer.Bool(true),
			}
			if _, err := lb.rpcClient.UpdateLoadBalancer(ctx, updateReq); err != nil {
				klog.Errorf("Failed to update load balancer via RPC: %v", err)
				return nil, stdErrors.New(rpc.ToRPCError(err))
			}

			lb.recordListenerHash(ctx, service, listeners)

			lbStatus, err := lb.pollLoadBalancerReady(ctx, service, lbID)
			if err != nil {
				return nil, err
			}
			lb.updateKamajiAdvertiseAddress(ctx, service, lbStatus.Ip)
			lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
			return lb.loadBalancerStatusFromResponse(service, lbStatus), nil
		}

		if lb.isFipEnabled(service) && lbStatus.FipState != loadbalancerv1.FipState_FIP_STATE_ACTIVE {
			lbStatus, err := lb.pollLoadBalancerReady(ctx, service, lbID)
			if err != nil {
				return nil, err
			}
			lb.updateKamajiAdvertiseAddress(ctx, service, lbStatus.Ip)
			lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
			return lb.loadBalancerStatusFromResponse(service, lbStatus), nil
		}

		lb.clearRetryCount(string(service.UID))

		return lb.loadBalancerStatusFromResponse(service, lbStatus), nil
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

		if lb.isFipEnabled(service) && lb.config.FipNetworkID == "" {
			return nil, fmt.Errorf("FIP network ID is not configured; load balancer recreation requires floating IP support")
		}

		lb.rpcClient.DeleteLoadBalancer(ctx, &loadbalancerv1.DeleteLoadBalancerRequest{Id: lbID})

		tenantID := lb.getTenantID(service)

		createReq := &loadbalancerv1.CreateLoadBalancerRequest{
			Name: lbName,
			Spec: &loadbalancerv1.LoadBalancerSpec{
				TenantId:              tenantID,
				NetworkId:             lb.getNetworkID(service),
				SubnetId:              lb.getSubnetID(service),
				Listeners:             listeners,
				ClientConnLimit:       defaultClientConnLimit,
				SecurityGroupDisabled: true,
				QosPolicyDisabled:     true,
			},
			IdempotencyKey: createIdempotencyKey(service, createCount+1),
		}

		if lb.isFipEnabled(service) && lb.config.FipNetworkID != "" {
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
		if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerCreateCountAnnotationKey, strconv.Itoa(createCount+1)); err != nil {
			klog.Errorf("Failed to record create count on service %s/%s: %v", service.Namespace, service.Name, err)
		}
		lb.recordListenerHash(ctx, service, listeners)

		lb.clearRetryCount(string(service.UID))

		lbStatus, err := lb.pollLoadBalancerReady(ctx, service, createResp.Id)
		if err != nil {
			return nil, err
		}
		lb.updateKamajiAdvertiseAddress(ctx, service, lbStatus.Ip)
		lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
		return lb.loadBalancerStatusFromResponse(service, lbStatus), nil
	}

	if lb.configChanged(service, listeners) {
		updateReq := &loadbalancerv1.UpdateLoadBalancerRequest{
			Id:                    lbID,
			Listeners:             mergeListenerIDs(listeners, lbStatus.GetSpec().GetListeners()),
			SecurityGroupDisabled: pointer.Bool(true),
			QosPolicyDisabled:     pointer.Bool(true),
		}
		if _, err := lb.rpcClient.UpdateLoadBalancer(ctx, updateReq); err != nil {
			klog.Errorf("Failed to update load balancer via RPC: %v", err)
			lb.incrementRetryCount(string(service.UID))
			return nil, stdErrors.New(rpc.ToRPCError(err))
		}

		lb.recordListenerHash(ctx, service, listeners)

		lbStatus, err := lb.pollLoadBalancerReady(ctx, service, lbID)
		if err != nil {
			// This branch is reached only while the load balancer is not ready.
			// Without counting the attempt, a load balancer whose config keeps
			// changing while it fails to provision would never reach the
			// recreate path above.
			lb.incrementRetryCount(string(service.UID))
			return nil, err
		}
		lb.clearRetryCount(string(service.UID))
		lb.updateKamajiAdvertiseAddress(ctx, service, lbStatus.Ip)
		lb.storeInternalIpIfBothMode(ctx, service, lbStatus)
		return lb.loadBalancerStatusFromResponse(service, lbStatus), nil
	}

	lb.incrementRetryCount(string(service.UID))

	return nil, fmt.Errorf("load balancer %s not ready%s, will retry", lbID, lbErrorDetail(lbStatus))
}

// lbErrorDetail renders the reason the LB API reported, when it reported one.
func lbErrorDetail(lbStatus *loadbalancerv1.LoadBalancer) string {
	if lbStatus == nil {
		return ""
	}
	if lbStatus.Error != "" {
		return fmt.Sprintf(" (state %s: %s)", lbStatus.State, lbStatus.Error)
	}
	return fmt.Sprintf(" (state %s)", lbStatus.State)
}

// createIdempotencyKey derives the key for one creation attempt. It must be
// stable across retries of the same attempt, so a lost response replays instead
// of leaking a second load balancer - and it must differ between attempts, or
// the server replays the previous (already deleted) creation and the recreate
// path silently does nothing. The key has to be a UUID per the proto contract.
func createIdempotencyKey(service *corev1.Service, createAttempt int) string {
	if createAttempt <= 0 {
		return string(service.UID)
	}
	space, err := uuid.Parse(string(service.UID))
	if err != nil {
		space = uuid.Nil
	}
	return uuid.NewSHA1(space, []byte(strconv.Itoa(createAttempt))).String()
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

func (lb *loadbalancer) incrementRetryCount(uid string) {
	lb.mu.Lock()
	lb.retryCounts[uid]++
	lb.mu.Unlock()
}

// recordListenerHash persists the config fingerprint change detection reads.
// A failure here is not fatal - the next sync recomputes it - but it has to be
// visible, because a hash that never lands makes every sync look like a change.
func (lb *loadbalancer) recordListenerHash(ctx context.Context, service *corev1.Service, listeners []*loadbalancerv1.Listener) {
	if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerListenerHashAnnotationKey, listenersHashStr(listeners)); err != nil {
		klog.Errorf("Failed to record listener hash on service %s/%s: %v", service.Namespace, service.Name, err)
	}
}

func (lb *loadbalancer) getEffectiveIpType(service *corev1.Service) string {
	if ipType, ok := service.Annotations[ServiceIpTypeAnnotationKey]; ok && ipType != "" {
		return strings.ToLower(ipType)
	}
	return strings.ToLower(lb.config.IpType)
}

func (lb *loadbalancer) isFipEnabled(service *corev1.Service) bool {
	ipType := lb.getEffectiveIpType(service)
	return ipType == "" || ipType == "external" || ipType == "both"
}

func (lb *loadbalancer) isInternalMode(service *corev1.Service) bool {
	return lb.getEffectiveIpType(service) == "internal"
}

func (lb *loadbalancer) isBothMode(service *corev1.Service) bool {
	return lb.getEffectiveIpType(service) == "both"
}

func (lb *loadbalancer) storeInternalIpIfBothMode(ctx context.Context, service *corev1.Service, lbStatus *loadbalancerv1.LoadBalancer) {
	if lb.isBothMode(service) && lbStatus != nil && lbStatus.Ip != "" {
		if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerInternalIPAnnotationKey, lbStatus.Ip); err != nil {
			klog.Errorf("Failed to store internal IP annotation: %v", err)
		}
	}
}

func (lb *loadbalancer) useAnnotationConfig() bool {
	return lb.config.OnlyServiceController
}

func (lb *loadbalancer) shouldSkipService(service *corev1.Service) bool {
	if !lb.useAnnotationConfig() {
		return false
	}
	return service.Annotations[ServiceNetworkIDAnnotationKey] == "" || service.Annotations[ServiceSubnetIDAnnotationKey] == ""
}

func (lb *loadbalancer) getNetworkID(service *corev1.Service) string {
	if lb.useAnnotationConfig() {
		return service.Annotations[ServiceNetworkIDAnnotationKey]
	}
	return lb.config.NetworkID
}

func (lb *loadbalancer) getSubnetID(service *corev1.Service) string {
	if lb.useAnnotationConfig() {
		return service.Annotations[ServiceSubnetIDAnnotationKey]
	}
	return lb.config.SubnetID
}

func (lb *loadbalancer) getTenantID(service *corev1.Service) string {
	if lb.useAnnotationConfig() {
		if tid, ok := service.Annotations[ServiceTenantIDAnnotationKey]; ok && tid != "" {
			return tid
		}
		return service.Namespace
	}
	if lb.config.TenantID != "" {
		return lb.config.TenantID
	}
	return service.Namespace
}

func (lb *loadbalancer) updateKamajiAdvertiseAddress(ctx context.Context, service *corev1.Service, internalIP string) {
	if !lb.useAnnotationConfig() || internalIP == "" {
		return
	}

	kcpName, ok := service.Labels["kamaji.clastix.io/name"]
	if !ok || kcpName == "" {
		return
	}

	patchObj := map[string]interface{}{
		"spec": map[string]interface{}{
			"network": map[string]interface{}{
				"advertiseAddress": internalIP,
			},
		},
	}
	patchBytes, err := json.Marshal(patchObj)
	if err != nil {
		klog.Warningf("Failed to marshal KamajiControlPlane patch: %v", err)
		return
	}

	kcp := &unstructured.Unstructured{}
	kcp.SetAPIVersion("controlplane.cluster.x-k8s.io/v1alpha2")
	kcp.SetKind("KamajiControlPlane")
	kcp.SetName(kcpName)
	kcp.SetNamespace(service.Namespace)

	if err := lb.tenantClient.Patch(ctx, kcp, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
		klog.Warningf("Failed to update KamajiControlPlane advertiseAddress: %v", err)
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

func (lb *loadbalancer) loadBalancerStatusFromResponse(service *corev1.Service, lbStatus *loadbalancerv1.LoadBalancer) *corev1.LoadBalancerStatus {
	if lbStatus == nil {
		return nil
	}
	ip := lbStatus.Ip
	if lb.isFipEnabled(service) && lbStatus.Fip != "" {
		ip = lbStatus.Fip
	}
	if ip == "" {
		return nil
	}
	ipMode := corev1.LoadBalancerIPModeVIP
	if lb.useAnnotationConfig() && lb.isInternalMode(service) {
		ipMode = corev1.LoadBalancerIPModeProxy
	}
	return &corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{{IP: ip, IPMode: &ipMode}},
	}
}

// buildListeners renders the Service as one listener per port. Each listener
// carries exactly one rule and each rule exactly one backend, whose endpoints
// are the <node ip>:<node port> pairs traffic is spread across. The API allows
// several rules per HTTP listener and several weighted backends per rule; a
// Kubernetes Service has nothing to express with either, so this stays at the
// shape TCP and UDP listeners are restricted to anyway.
func (lb *loadbalancer) buildListeners(ctx context.Context, service *corev1.Service, nodes []*corev1.Node, clusterName string) []*loadbalancerv1.Listener {
	listeners := make([]*loadbalancerv1.Listener, 0, len(service.Spec.Ports))

	for _, port := range service.Spec.Ports {
		protocol := listenerProtocol(service, port)

		rule := &loadbalancerv1.ListenerRule{
			Algorithm: defaultAlgorithm,
			Backends: []*loadbalancerv1.RuleBackend{{
				Name:      defaultBackendName,
				Weight:    defaultBackendWeight,
				Endpoints: lb.buildEndpoints(ctx, port, nodes, clusterName),
			}},
		}

		if protocol == loadbalancerv1.Protocol_PROTOCOL_HTTP {
			healthCheckPath := defaultHealthCheckPath
			healthCheckMethod := loadbalancerv1.HttpMethod_HTTP_METHOD_GET
			match := &loadbalancerv1.HttpRouteMatch{}
			matched := false
			if path, ok := service.Annotations[HTTPRoutePathAnnotationKey]; ok && path != "" {
				healthCheckPath = path
				match.Path = &loadbalancerv1.HttpPathMatch{
					Type:  loadbalancerv1.HttpPathType_HTTP_PATH_TYPE_PREFIX,
					Value: path,
				}
				matched = true
			}
			if method, ok := service.Annotations[HTTPRouteMethodAnnotationKey]; ok && method != "" {
				healthCheckMethod = lb.parseHTTPMethod(method)
				match.Method = healthCheckMethod
				matched = true
			}
			// A match has to constrain something. An empty match list is the
			// legal way to say "everything"; an empty match is rejected.
			if matched {
				rule.Matches = []*loadbalancerv1.HttpRouteMatch{match}
			}
			rule.HealthMonitor = &loadbalancerv1.HealthMonitor{
				Interval:              defaultHealthMonitorInterval,
				Timeout:               defaultHealthMonitorTimeout,
				UnhealthyThreshold:    defaultHealthMonitorUnhealthy,
				HealthyThreshold:      defaultHealthMonitorHealthy,
				HttpHealthCheckPath:   healthCheckPath,
				HttpHealthCheckMethod: healthCheckMethod,
				ExpectedStatusCodes:   []int32{200},
			}
		} else {
			rule.HealthMonitor = &loadbalancerv1.HealthMonitor{
				Interval:           defaultHealthMonitorInterval,
				Timeout:            defaultHealthMonitorTimeout,
				UnhealthyThreshold: defaultHealthMonitorUnhealthy,
				HealthyThreshold:   defaultHealthMonitorHealthy,
			}
		}

		listeners = append(listeners, &loadbalancerv1.Listener{
			Port:       port.Port,
			Protocol:   protocol,
			TlsEnabled: false,
			Rules:      []*loadbalancerv1.ListenerRule{rule},
		})
	}

	return listeners
}

// listenerProtocol picks the listener protocol for one Service port. HTTP is
// opt-in through the route annotation and only applies on top of TCP; SCTP has
// no counterpart in the API and is served as a plain TCP stream.
func listenerProtocol(service *corev1.Service, port corev1.ServicePort) loadbalancerv1.Protocol {
	if port.Protocol == corev1.ProtocolUDP {
		return loadbalancerv1.Protocol_PROTOCOL_UDP
	}
	if _, hasHTTPRoute := service.Annotations[HTTPRoutePathAnnotationKey]; hasHTTPRoute {
		return loadbalancerv1.Protocol_PROTOCOL_HTTP
	}
	return loadbalancerv1.Protocol_PROTOCOL_TCP
}

// mergeListenerIDs returns desired with the server-assigned identifiers found in
// current grafted on, matching listeners by port and protocol.
//
// The API reconciles an update by id alone: a listener sent without one is
// inserted fresh and the row it replaces - along with its rules, its backends
// and the data plane objects generated from them - is deleted. UpdateLoadBalancer
// runs on every node that joins or leaves the cluster, so dropping the ids would
// rebuild the whole listener each time the endpoint list moves.
//
// desired is left untouched; the ids are grafted onto clones.
func mergeListenerIDs(desired, current []*loadbalancerv1.Listener) []*loadbalancerv1.Listener {
	if len(desired) == 0 {
		return desired
	}

	existing := make(map[string]*loadbalancerv1.Listener, len(current))
	for _, l := range current {
		if l != nil {
			existing[listenerKey(l)] = l
		}
	}

	merged := make([]*loadbalancerv1.Listener, 0, len(desired))
	for _, want := range desired {
		clone := proto.Clone(want).(*loadbalancerv1.Listener)
		merged = append(merged, clone)

		have := existing[listenerKey(clone)]
		if have == nil {
			continue
		}
		clone.Id = have.GetId()

		// One rule with one backend on both sides, so position is identity.
		// Anything else came from outside this controller and is left to the
		// server's own reconciliation.
		if len(clone.GetRules()) != 1 || len(have.GetRules()) != 1 {
			continue
		}
		wantRule, haveRule := clone.GetRules()[0], have.GetRules()[0]
		wantRule.Id = haveRule.GetId()

		if len(wantRule.GetBackends()) != 1 || len(haveRule.GetBackends()) != 1 {
			continue
		}
		wantRule.GetBackends()[0].Id = haveRule.GetBackends()[0].GetId()
	}

	return merged
}

func listenerKey(l *loadbalancerv1.Listener) string {
	return fmt.Sprintf("%d/%s", l.GetPort(), l.GetProtocol())
}

// validateListeners rejects a payload the API would reject anyway, so the reason
// reaches the Service event instead of arriving as a generic InvalidArgument.
func validateListeners(listeners []*loadbalancerv1.Listener) error {
	if len(listeners) == 0 {
		return fmt.Errorf("service has no ports to load balance")
	}
	for _, l := range listeners {
		for _, rule := range l.GetRules() {
			for _, backend := range rule.GetBackends() {
				if len(backend.GetEndpoints()) == 0 {
					return fmt.Errorf("no backend endpoints available for port %d; no node addresses and no matching VMI IPs were found", l.GetPort())
				}
			}
		}
	}
	return nil
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

// buildEndpoints resolves the upstream addresses for one Service port. The
// result is deduplicated and ordered: the API rejects a backend that lists the
// same ip:port twice, and a stable order keeps the listener hash from changing
// just because the node list came back in a different sequence.
//
// Addresses are filtered through isUsableNodeIP. A node this CCM manages should
// never carry a link-local address, but in the infra-cluster deployment the node
// list comes from another cloud provider entirely, and an unroutable endpoint is
// not merely useless: it makes the whole load balancer fail to provision.
func (lb *loadbalancer) buildEndpoints(ctx context.Context, port corev1.ServicePort, nodes []*corev1.Node, clusterName string) []*loadbalancerv1.BackendRef {
	endpoints := make([]*loadbalancerv1.BackendRef, 0)

	if len(nodes) > 0 {
		for _, node := range nodes {
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeInternalIP && isUsableNodeIP(addr.Address) {
					endpoints = append(endpoints, &loadbalancerv1.BackendRef{
						Ip:   addr.Address,
						Port: port.NodePort,
					})
				}
			}
		}

		if len(endpoints) == 0 {
			klog.Warningf("No internal IPs found for backend endpoints, trying external IPs")
			for _, node := range nodes {
				for _, addr := range node.Status.Addresses {
					if addr.Type == corev1.NodeExternalIP && isUsableNodeIP(addr.Address) {
						endpoints = append(endpoints, &loadbalancerv1.BackendRef{
							Ip:   addr.Address,
							Port: port.NodePort,
						})
					}
				}
			}
		}
	}

	if len(endpoints) == 0 && clusterName != "" {
		klog.Warningf("No backend IPs from nodes, falling back to VMI IPs for cluster %s", clusterName)
		endpoints = lb.buildEndpointsFromVMI(ctx, port, clusterName)
	}

	return normalizeEndpoints(endpoints)
}

// normalizeEndpoints drops duplicate ip:port pairs and sorts what is left.
func normalizeEndpoints(endpoints []*loadbalancerv1.BackendRef) []*loadbalancerv1.BackendRef {
	seen := make(map[string]struct{}, len(endpoints))
	unique := make([]*loadbalancerv1.BackendRef, 0, len(endpoints))
	for _, e := range endpoints {
		key := fmt.Sprintf("%s:%d", e.GetIp(), e.GetPort())
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, e)
	}

	sort.Slice(unique, func(i, j int) bool {
		if unique[i].GetIp() != unique[j].GetIp() {
			return unique[i].GetIp() < unique[j].GetIp()
		}
		return unique[i].GetPort() < unique[j].GetPort()
	})

	return unique
}

func (lb *loadbalancer) buildEndpointsFromVMI(ctx context.Context, port corev1.ServicePort, clusterName string) []*loadbalancerv1.BackendRef {
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
		if ip := defaultInterfaceIP(vmi.Status.Interfaces); ip != "" {
			backendRefs = append(backendRefs, &loadbalancerv1.BackendRef{
				Ip:   ip,
				Port: port.NodePort,
			})
		}
	}

	if len(backendRefs) == 0 {
		klog.Warningf("No VMI IPs found for cluster %s", clusterName)
	}

	return backendRefs
}

// defaultInterfaceIP picks the address traffic to a VMI should be sent to. Only
// the "default" interface is the pod network - the rest are whatever the CNI
// inside the guest created, so a VMI running Cilium reports cilium_host on the
// pod CIDR and a link-local for every lxc* veth. The guest agent does not order
// that list, so anything less specific picks a different address run to run.
func defaultInterfaceIP(ifs []kubevirtv1.VirtualMachineInstanceNetworkInterface) string {
	for _, iface := range ifs {
		if iface.Name != "default" {
			continue
		}
		for _, ip := range iface.IPs {
			if isUsableNodeIP(ip) {
				return ip
			}
		}
		if isUsableNodeIP(iface.IP) {
			return iface.IP
		}
		return ""
	}
	return ""
}

// UpdateLoadBalancer updates hosts under the specified load balancer.
func (lb *loadbalancer) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	if lb.rpcClient == nil {
		return fmt.Errorf("load balancer RPC client is not configured")
	}

	// Node syncs fan out to every LoadBalancer Service in the cluster, including
	// the ones EnsureLoadBalancer declined. Without this the missing id below is
	// reported as an error for each of them on every node event.
	if lb.shouldSkipService(service) && service.Annotations[LoadBalancerIDAnnotationKey] == "" {
		return nil
	}

	lbID := service.Annotations[LoadBalancerIDAnnotationKey]
	if lbID == "" {
		return fmt.Errorf("load balancer ID not found in service annotations")
	}

	listeners := lb.buildListeners(ctx, service, nodes, clusterName)
	if err := validateListeners(listeners); err != nil {
		return err
	}

	// The service controller calls this on every node add and remove, whether or
	// not the node affects this Service. Without the guard each of those syncs
	// sends an update that changes nothing and pushes the load balancer through
	// another provisioning cycle.
	if !lb.configChanged(service, listeners) {
		return nil
	}

	getResp, err := lb.rpcClient.GetLoadBalancer(ctx, &loadbalancerv1.GetLoadBalancerRequest{Id: lbID})
	if err != nil {
		klog.Errorf("Failed to read load balancer before update: %v", err)
		return stdErrors.New(rpc.ToRPCError(err))
	}

	if _, err := lb.rpcClient.UpdateLoadBalancer(ctx, &loadbalancerv1.UpdateLoadBalancerRequest{
		Id:                    lbID,
		Listeners:             mergeListenerIDs(listeners, getResp.GetLoadBalancer().GetSpec().GetListeners()),
		SecurityGroupDisabled: pointer.Bool(true),
		QosPolicyDisabled:     pointer.Bool(true),
	}); err != nil {
		klog.Errorf("Failed to update load balancer via RPC: %v", err)
		return stdErrors.New(rpc.ToRPCError(err))
	}

	// EnsureLoadBalancer reads this annotation to decide whether the config
	// drifted; leaving it stale would make the next Ensure send the same update
	// a second time.
	if err := lb.ensureServiceAnnotation(ctx, service, LoadBalancerListenerHashAnnotationKey, listenersHashStr(listeners)); err != nil {
		klog.Errorf("Failed to record listener hash after update: %v", err)
	}

	return nil
}

// EnsureLoadBalancerDeleted deletes the specified load balancer if it exists.
func (lb *loadbalancer) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	// Reporting success without a client would let the service controller drop
	// the cleanup finalizer and leave the load balancer behind with nothing left
	// pointing at it.
	if lb.rpcClient == nil {
		return fmt.Errorf("load balancer RPC client is not configured")
	}

	// A Service this CCM declined has nothing to clean up - unless it carries an
	// id, which means it was ours before its config annotations were stripped.
	if lb.shouldSkipService(service) && service.Annotations[LoadBalancerIDAnnotationKey] == "" {
		return nil
	}

	defer lb.clearRetryCount(string(service.UID))

	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)
	lbID := service.Annotations[LoadBalancerIDAnnotationKey]

	if lbID == "" {
		found, err := lb.findLoadBalancerByName(ctx, lb.getTenantID(service), lbName)
		if err != nil {
			klog.Errorf("Failed to look up load balancer %s by name: %v", lbName, err)
			return stdErrors.New(rpc.ToRPCError(err))
		}
		lbID = found
	}

	if lbID == "" {
		return nil
	}

	if _, err := lb.rpcClient.DeleteLoadBalancer(ctx, &loadbalancerv1.DeleteLoadBalancerRequest{Id: lbID}); err != nil {
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.NotFound {
			klog.Errorf("Failed to delete load balancer via RPC: %v", err)
			return stdErrors.New(rpc.ToRPCError(err))
		}
		klog.Infof("Load balancer %s already deleted", lbID)
	}

	return nil
}

// findLoadBalancerByName is the fallback for a Service whose id annotation never
// made it back - without it that load balancer is orphaned on delete. An error
// is returned rather than swallowed for the same reason.
func (lb *loadbalancer) findLoadBalancerByName(ctx context.Context, tenantID, lbName string) (string, error) {
	pageToken := ""
	for {
		resp, err := lb.rpcClient.ListLoadBalancers(ctx, &loadbalancerv1.ListLoadBalancersRequest{
			TenantId: tenantID,
			// The API rejects a page size below 1, so leaving this unset made the
			// whole lookup fail on validation.
			PageSize:  listLoadBalancersPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return "", err
		}

		for _, summary := range resp.GetLoadBalancers() {
			if summary.GetName() == lbName {
				klog.Infof("Found load balancer by name %s with ID %s", lbName, summary.GetId())
				return summary.GetId(), nil
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return "", nil
		}
	}
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

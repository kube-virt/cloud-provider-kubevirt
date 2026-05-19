package provider

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	lbv1 "github.com/kube-virt/loadbalancer-api/gen/loadbalancer/v1"
	"github.com/kube-virt/loadbalancer-api/gen/loadbalancer/v1/loadbalancerv1connect"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Default interval between polling the service after creation
	defaultLoadBalancerCreatePollInterval = 10 * time.Second

	// Default timeout between polling the service after creation
	defaultLoadBalancerCreatePollTimeout = 5 * time.Minute

	TenantServiceNameLabelKey      = "cluster.x-k8s.io/tenant-service-name"
	TenantServiceNamespaceLabelKey = "cluster.x-k8s.io/tenant-service-namespace"
	TenantClusterNameLabelKey      = "cluster.x-k8s.io/cluster-name"
	TenantNodeRoleLabelKey         = "cluster.x-k8s.io/role"
)

type loadbalancer struct {
	namespace   string
	client      client.Client
	config      LoadBalancerConfig
	infraLabels map[string]string
	lbClient    loadbalancerv1connect.LoadBalancerServiceClient
}

// newLBClient creates a connect-rpc LoadBalancer API client.
func newLBClient(serverAddr string) loadbalancerv1connect.LoadBalancerServiceClient {
	return loadbalancerv1connect.NewLoadBalancerServiceClient(
		http.DefaultClient,
		serverAddr,
	)
}

// GetLoadBalancer returns whether the specified load balancer exists, and
// if so, what its status is.
// Implementations must treat the *v1.Service parameter as read-only and not modify it.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (status *corev1.LoadBalancerStatus, exists bool, err error) {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)

	summary, err := lb.findLBByName(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to get LoadBalancer %s: %v", lbName, err)
		return nil, false, err
	}
	if summary == nil {
		return nil, false, nil
	}

	return lbStatusFromSummary(summary), true, nil
}

// GetLoadBalancerName is an implementation of LoadBalancer.GetLoadBalancerName.
func (lb *loadbalancer) GetLoadBalancerName(ctx context.Context, clusterName string, service *corev1.Service) string {
	return cloudprovider.DefaultLoadBalancerName(service)
}

// EnsureLoadBalancer creates a new load balancer 'name', or updates the existing one. Returns the status of the balancer.
// Implementations must treat the *v1.Service and *v1.Node
// parameters as read-only and not modify them.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) EnsureLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) (*corev1.LoadBalancerStatus, error) {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)

	summary, err := lb.findLBByName(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to look up LoadBalancer %s: %v", lbName, err)
		return nil, err
	}

	if summary != nil {
		switch summary.State {
		case lbv1.State_STATE_READY:
			// If ports or protocol changed, recreate; otherwise return current status.
			if lb.portsChanged(service, summary) {
				klog.Infof("Ports changed for LB %s, recreating", lbName)
				if err := lb.deleteLB(ctx, summary.Id); err != nil {
					return nil, err
				}
				// Fall through to create below.
			} else {
				// LB is ready — ensure FIP is allocated and return status.
				return lb.ensureFIPForID(ctx, summary.Id, summary.FipState, summary.Fip, summary.Ip)
			}
		case lbv1.State_STATE_FAILED:
			klog.Infof("LB %s is in FAILED state, recreating", lbName)
			if err := lb.deleteLB(ctx, summary.Id); err != nil {
				return nil, err
			}
			// Fall through to create below.
		case lbv1.State_STATE_DELETING, lbv1.State_STATE_PENDING, lbv1.State_STATE_PROVISIONING:
			// LB is transitioning — poll until READY.
			return lb.pollUntilReady(ctx, summary.Id)
		}
	}

	// Create new LB.
	lbID, err := lb.createLB(ctx, lbName, service, nodes)
	if err != nil {
		klog.Errorf("Failed to create LoadBalancer %s: %v", lbName, err)
		return nil, err
	}

	// Poll until the LB is READY, then ensure FIP.
	return lb.pollUntilReady(ctx, lbID)
}

// UpdateLoadBalancer updates the load balancer if ports have changed.
// Implementations must treat the *v1.Service and *v1.Node
// parameters as read-only and not modify them.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)

	summary, err := lb.findLBByName(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to look up LoadBalancer %s: %v", lbName, err)
		return err
	}
	if summary == nil {
		klog.Warningf("LoadBalancer %s not found during UpdateLoadBalancer, nothing to update", lbName)
		return nil
	}

	if !lb.portsChanged(service, summary) {
		return nil
	}

	klog.Infof("Ports changed for LB %s, recreating", lbName)
	if err := lb.deleteLB(ctx, summary.Id); err != nil {
		return err
	}
	_, err = lb.EnsureLoadBalancer(ctx, clusterName, service, nodes)
	return err
}

// EnsureLoadBalancerDeleted deletes the specified load balancer if it exists,
// returning nil if the load balancer specified either didn't exist or was successfully deleted.
// Implementations must treat the *v1.Service parameter as read-only and not modify it.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)

	summary, err := lb.findLBByName(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to look up LoadBalancer %s: %v", lbName, err)
		return err
	}
	if summary == nil {
		return nil
	}

	if err := lb.deleteLB(ctx, summary.Id); err != nil {
		klog.Errorf("Failed to delete LoadBalancer %s (id=%s): %v", lbName, summary.Id, err)
		return err
	}
	return nil
}

// ---- internal helpers ----

// createLB calls the LB API to create a new load balancer and returns its ID.
func (lb *loadbalancer) createLB(ctx context.Context, name string, service *corev1.Service, nodes []*corev1.Node) (string, error) {
	if len(service.Spec.Ports) == 0 {
		return "", fmt.Errorf("service %s/%s has no ports", service.Namespace, service.Name)
	}

	port := service.Spec.Ports[0]
	backendRefs := lb.buildBackendRefs(nodes, int32(port.NodePort))

	spec := &lbv1.LoadBalancerSpec{
		TenantId:    lb.config.TenantID,
		NetworkId:   lb.config.NetworkID,
		SubnetId:    lb.config.SubnetID,
		ListenPort:  port.Port,
		Protocol:    effectiveProtocol(service, port),
		BackendRefs: backendRefs,
		Algorithm:   algorithmFromString(lb.config.Algorithm),
	}

	resp, err := lb.lbClient.CreateLoadBalancer(ctx, connect.NewRequest(&lbv1.CreateLoadBalancerRequest{
		Name: name,
		Spec: spec,
	}))
	if err != nil {
		return "", err
	}
	klog.Infof("LoadBalancer %s created with id=%s state=%s", name, resp.Msg.Id, resp.Msg.State)
	return resp.Msg.Id, nil
}

// deleteLB calls the LB API to delete a load balancer by ID.
func (lb *loadbalancer) deleteLB(ctx context.Context, id string) error {
	_, err := lb.lbClient.DeleteLoadBalancer(ctx, connect.NewRequest(&lbv1.DeleteLoadBalancerRequest{
		Id: id,
	}))
	if err != nil && !lbIsNotFound(err) {
		return err
	}
	return nil
}

// findLBByName searches the LB API for a load balancer matching the given name.
// Returns nil if not found or already deleted.
func (lb *loadbalancer) findLBByName(ctx context.Context, name string) (*lbv1.LoadBalancerSummary, error) {
	resp, err := lb.lbClient.ListLoadBalancers(ctx, connect.NewRequest(&lbv1.ListLoadBalancersRequest{
		TenantId: lb.config.TenantID,
	}))
	if err != nil {
		return nil, err
	}
	for _, s := range resp.Msg.LoadBalancers {
		if s.Name == name && s.State != lbv1.State_STATE_DELETED {
			return s, nil
		}
	}
	return nil, nil
}

// pollUntilReady polls the LB API until the LB reaches READY state,
// then ensures a floating IP is allocated, and returns the LoadBalancerStatus.
func (lb *loadbalancer) pollUntilReady(ctx context.Context, lbID string) (*corev1.LoadBalancerStatus, error) {
	var readyLB *lbv1.LoadBalancer

	err := wait.PollWithContext(ctx, lb.getLoadBalancerCreatePollInterval(), lb.getLoadBalancerCreatePollTimeout(), func(ctx context.Context) (bool, error) {
		resp, err := lb.lbClient.GetLoadBalancer(ctx, connect.NewRequest(&lbv1.GetLoadBalancerRequest{Id: lbID}))
		if err != nil {
			klog.Warningf("Error polling LoadBalancer %s: %v", lbID, err)
			return false, nil
		}
		remLB := resp.Msg.LoadBalancer
		klog.V(4).Infof("Polling LB %s: state=%s", lbID, remLB.State)
		switch remLB.State {
		case lbv1.State_STATE_READY:
			readyLB = remLB
			return true, nil
		case lbv1.State_STATE_FAILED:
			return false, fmt.Errorf("LoadBalancer %s entered FAILED state: %s", lbID, remLB.Error)
		case lbv1.State_STATE_DELETED:
			return false, fmt.Errorf("LoadBalancer %s was unexpectedly deleted", lbID)
		}
		return false, nil
	})
	if err != nil {
		klog.Errorf("Failed waiting for LoadBalancer %s to become ready: %v", lbID, err)
		return nil, err
	}

	return lb.ensureFIPForID(ctx, readyLB.Id, readyLB.FipState, readyLB.Fip, readyLB.Ip)
}

// ensureFIPForID allocates a floating IP (if needed) for the given LB ID
// and polls until active, returning the LoadBalancerStatus with the external IP.
func (lb *loadbalancer) ensureFIPForID(ctx context.Context, lbID string, fipState lbv1.FipState, fip, ip string) (*corev1.LoadBalancerStatus, error) {
	// If FIP already active, return immediately.
	if fipState == lbv1.FipState_FIP_STATE_ACTIVE && fip != "" {
		return lbStatusFromIP(fip, ip), nil
	}

	// Request FIP allocation if not yet started.
	if fipState == lbv1.FipState_FIP_STATE_NONE || fipState == lbv1.FipState_FIP_STATE_UNSPECIFIED {
		klog.Infof("Allocating floating IP for LB %s", lbID)
		_, err := lb.lbClient.AllocateFloatingIp(ctx, connect.NewRequest(&lbv1.AllocateFloatingIpRequest{
			Id:           lbID,
			Fip:          "",
			FipNetworkId: lb.config.FipNetworkID,
		}))
		if err != nil {
			klog.Errorf("Failed to allocate floating IP for LB %s: %v", lbID, err)
			return nil, err
		}
	}

	// Poll until FIP becomes active.
	var externalIP string
	err := wait.PollWithContext(ctx, lb.getLoadBalancerCreatePollInterval(), lb.getLoadBalancerCreatePollTimeout(), func(ctx context.Context) (bool, error) {
		resp, err := lb.lbClient.GetLoadBalancer(ctx, connect.NewRequest(&lbv1.GetLoadBalancerRequest{Id: lbID}))
		if err != nil {
			klog.Warningf("Error polling FIP state for LB %s: %v", lbID, err)
			return false, nil
		}
		current := resp.Msg.LoadBalancer
		klog.V(4).Infof("Polling FIP for LB %s: fipState=%s fip=%s", lbID, current.FipState, current.Fip)
		if current.FipState == lbv1.FipState_FIP_STATE_ACTIVE {
			if current.Fip != "" {
				externalIP = current.Fip
			} else {
				externalIP = current.Ip
			}
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		klog.Errorf("Failed waiting for floating IP on LB %s: %v", lbID, err)
		return nil, err
	}

	return &corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{{IP: externalIP}},
	}, nil
}

// portsChanged returns true if the service's desired port/protocol differ from the existing LB summary.
func (lb *loadbalancer) portsChanged(service *corev1.Service, summary *lbv1.LoadBalancerSummary) bool {
	if len(service.Spec.Ports) == 0 {
		return false
	}
	port := service.Spec.Ports[0]
	desiredHash := portsHashFromService(service)
	existingHash := portsHashFromSummary(summary)
	_ = desiredHash
	_ = existingHash
	// Compare listen port and protocol directly from the summary.
	desiredProtocol := effectiveProtocol(service, port)
	return summary.Port != port.Port || summary.Protocol != desiredProtocol
}

// buildBackendRefs converts a list of nodes to LB backend references.
func (lb *loadbalancer) buildBackendRefs(nodes []*corev1.Node, nodePort int32) []*lbv1.BackendRef {
	var refs []*lbv1.BackendRef
	for _, node := range nodes {
		ip := nodeInternalIPFromNode(node)
		if ip == "" {
			continue
		}
		refs = append(refs, &lbv1.BackendRef{Ip: ip, Port: nodePort})
	}
	return refs
}

func nodeInternalIPFromNode(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	return ""
}

// lbStatusFromSummary converts a LoadBalancerSummary to a Kubernetes LoadBalancerStatus.
func lbStatusFromSummary(s *lbv1.LoadBalancerSummary) *corev1.LoadBalancerStatus {
	return lbStatusFromIP(s.Fip, s.Ip)
}

// lbStatusFromIP builds a LoadBalancerStatus preferring the floating IP over the internal IP.
func lbStatusFromIP(fip, ip string) *corev1.LoadBalancerStatus {
	external := fip
	if external == "" {
		external = ip
	}
	if external == "" {
		return &corev1.LoadBalancerStatus{}
	}
	return &corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{{IP: external}},
	}
}

// portsHashFromService computes a stable hash of the service ports for change detection.
func portsHashFromService(svc *corev1.Service) string {
	type portKey struct {
		Port     int32
		NodePort int32
		Protocol string
	}
	keys := make([]portKey, 0, len(svc.Spec.Ports))
	for _, p := range svc.Spec.Ports {
		keys = append(keys, portKey{
			Port:     p.Port,
			NodePort: p.NodePort,
			Protocol: effectiveProtocol(svc, p),
		})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Port < keys[j].Port })
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("%d/%d/%s|", k.Port, k.NodePort, k.Protocol))
	}
	h := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("%x", h[:8])
}

// portsHashFromSummary computes a hash from the LB summary for comparison.
func portsHashFromSummary(s *lbv1.LoadBalancerSummary) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d/%s", s.Port, s.Protocol)))
	return fmt.Sprintf("%x", h[:8])
}

// effectiveProtocol returns the protocol string to use for the LB API.
// It respects a "ccm.kube-virt.io/protocol" annotation on the Service.
func effectiveProtocol(svc *corev1.Service, port corev1.ServicePort) string {
	if ann, ok := svc.Annotations["ccm.kube-virt.io/protocol"]; ok {
		upper := strings.ToUpper(strings.TrimSpace(ann))
		switch upper {
		case "TCP", "UDP", "HTTP":
			return upper
		}
	}
	switch port.Protocol {
	case corev1.ProtocolUDP:
		return "UDP"
	default:
		return "TCP"
	}
}

// lbIsNotFound returns true if the connect-rpc error is a "not found" error.
func lbIsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if ce, ok := err.(*connect.Error); ok {
		return ce.Code() == connect.CodeNotFound
	}
	return strings.Contains(err.Error(), "not found")
}

// algorithmFromString converts an algorithm name string to the protobuf enum.
func algorithmFromString(s string) lbv1.Algorithm {
	switch strings.ToUpper(s) {
	case "LEAST_REQUEST":
		return lbv1.Algorithm_ALGORITHM_LEAST_REQUEST
	case "RANDOM":
		return lbv1.Algorithm_ALGORITHM_RANDOM
	case "CONSISTENT_HASH":
		return lbv1.Algorithm_ALGORITHM_CONSISTENT_HASH
	default:
		return lbv1.Algorithm_ALGORITHM_ROUND_ROBIN
	}
}

func (lb *loadbalancer) getLoadBalancerCreatePollInterval() time.Duration {
	return convertLoadBalancerCreatePollConfig(lb.config.CreationPollInterval, defaultLoadBalancerCreatePollInterval, "interval")
}

func (lb *loadbalancer) getLoadBalancerCreatePollTimeout() time.Duration {
	return convertLoadBalancerCreatePollConfig(lb.config.CreationPollTimeout, defaultLoadBalancerCreatePollTimeout, "timeout")
}

func convertLoadBalancerCreatePollConfig(configValue *int, defaultValue time.Duration, name string) time.Duration {
	if configValue == nil {
		klog.Infof("Setting creation poll %s to default value '%d'", name, defaultValue)
		return defaultValue
	}
	if *configValue <= 0 {
		klog.Infof("Creation poll %s %d' must be > 0. Setting to '%d'", name, *configValue, defaultValue)
		return defaultValue
	}
	return time.Duration(*configValue) * time.Second
}

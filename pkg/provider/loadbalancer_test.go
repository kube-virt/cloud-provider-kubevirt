package provider

import (
	"context"
	"errors"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mockclient "kubevirt.io/cloud-provider-kubevirt/pkg/provider/mock/client"
	loadbalancerv1 "kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer/gen"
)

const (
	lbServiceName      string = "af6ebf1722bb111e9b210d663bd873d9"
	lbServiceNamespace string = "test"
	clusterName        string = "kvcluster"
)

func makeLoadBalancerStatus(ips, hostnames []string) *corev1.LoadBalancerStatus {
	status := &corev1.LoadBalancerStatus{}
	lenIps := len(ips)
	lenHostnames := len(hostnames)
	var length int
	if lenIps > lenHostnames {
		length = lenIps
	} else {
		length = lenHostnames
	}
	if length == 0 {
		return status
	}
	ingressList := make([]corev1.LoadBalancerIngress, length)
	for i := 0; i < length; i++ {
		var ip, hostname string
		if i < lenIps {
			ip = ips[i]
		}
		if i < lenHostnames {
			hostname = hostnames[i]
		}
		ingressList[i] = corev1.LoadBalancerIngress{
			IP:       ip,
			Hostname: hostname,
		}
	}
	status.Ingress = ingressList
	return status
}

func cmpLoadBalancerStatuses(a, b *corev1.LoadBalancerStatus) bool {
	if a == b {
		return true
	}
	if (a == nil || b == nil) || (len(a.Ingress) != len(b.Ingress)) {
		return false
	}
	for i, aIngress := range a.Ingress {
		if (aIngress.IP != b.Ingress[i].IP) || (aIngress.Hostname != b.Ingress[i].Hostname) {
			return false
		}
	}
	return true
}

type fakeRPCClient struct {
	createResp   *loadbalancerv1.CreateLoadBalancerResponse
	createErr    error
	getResp      *loadbalancerv1.GetLoadBalancerResponse
	getErr       error
	updateResp   *loadbalancerv1.UpdateLoadBalancerResponse
	updateErr    error
	deleteResp   *loadbalancerv1.DeleteLoadBalancerResponse
	deleteErr    error
	listResp     *loadbalancerv1.ListLoadBalancersResponse
	listErr      error

	createReqs []*loadbalancerv1.CreateLoadBalancerRequest
	// getRespAfterCreate, when set, is returned by GetLoadBalancer once a
	// create has happened - i.e. the replacement LB the recreate path made.
	getRespAfterCreate *loadbalancerv1.GetLoadBalancerResponse
}

func (f *fakeRPCClient) CreateLoadBalancer(ctx context.Context, req *loadbalancerv1.CreateLoadBalancerRequest) (*loadbalancerv1.CreateLoadBalancerResponse, error) {
	f.createReqs = append(f.createReqs, req)
	return f.createResp, f.createErr
}
func (f *fakeRPCClient) GetLoadBalancer(ctx context.Context, req *loadbalancerv1.GetLoadBalancerRequest) (*loadbalancerv1.GetLoadBalancerResponse, error) {
	if f.getRespAfterCreate != nil && len(f.createReqs) > 0 {
		return f.getRespAfterCreate, nil
	}
	return f.getResp, f.getErr
}
func (f *fakeRPCClient) UpdateLoadBalancer(ctx context.Context, req *loadbalancerv1.UpdateLoadBalancerRequest) (*loadbalancerv1.UpdateLoadBalancerResponse, error) {
	return f.updateResp, f.updateErr
}
func (f *fakeRPCClient) DeleteLoadBalancer(ctx context.Context, req *loadbalancerv1.DeleteLoadBalancerRequest) (*loadbalancerv1.DeleteLoadBalancerResponse, error) {
	return f.deleteResp, f.deleteErr
}
func (f *fakeRPCClient) ListLoadBalancers(ctx context.Context, req *loadbalancerv1.ListLoadBalancersRequest) (*loadbalancerv1.ListLoadBalancersResponse, error) {
	return f.listResp, f.listErr
}

func newTestLoadBalancer(ctrl *gomock.Controller, rpc rpcLBClient) *loadbalancer {
	c := mockclient.NewMockClient(ctrl)
	tenantC := mockclient.NewMockClient(ctrl)
	return &loadbalancer{
		namespace:    "test",
		client:       c,
		tenantClient: tenantC,
		config: LoadBalancerConfig{
			CreationPollInterval: pointer.Int(1),
			CreationPollTimeout:  pointer.Int(5),
		},
		rpcClient: rpc,
	}
}

func newTestLoadBalancerWithTenantClient(ctrl *gomock.Controller, tenantC client.Client, rpc rpcLBClient) *loadbalancer {
	c := mockclient.NewMockClient(ctrl)
	return &loadbalancer{
		namespace:    "test",
		client:       c,
		tenantClient: tenantC,
		config: LoadBalancerConfig{
			CreationPollInterval: pointer.Int(1),
			CreationPollTimeout:  pointer.Int(5),
		},
		rpcClient: rpc,
	}
}

func newTenantService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "service1",
			Namespace: "test",
			UID:       types.UID("f6ebf172-2bb1-11e9-b210-d663bd873d93"),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{Name: "port1", Protocol: corev1.ProtocolTCP, Port: 80, NodePort: 30001},
			},
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeCluster,
		},
	}
}

var _ = Describe("LoadBalancer", func() {

	Context("With getting loadbalancer status", Ordered, func() {
		var (
			ctrl *gomock.Controller
			ctx  context.Context
			lb   *loadbalancer
		)

		BeforeAll(func() {
			ctrl, ctx = gomock.WithContext(context.Background(), GinkgoT())
			lb = newTestLoadBalancer(ctrl, nil)
		})

		It("Should return error when RPC client is nil", func() {
			svc := newTenantService()
			status, exists, err := lb.GetLoadBalancer(ctx, clusterName, svc)
			Expect(err).To(MatchError(ContainSubstring("RPC client is not configured")))
			Expect(exists).To(BeFalse())
			Expect(status).To(BeNil())
		})

		It("Should return not exists when no annotation is present", func() {
			fakeRPC := &fakeRPCClient{}
			lb.rpcClient = fakeRPC
			svc := newTenantService()
			status, exists, err := lb.GetLoadBalancer(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
			Expect(status).To(BeNil())
		})

		It("Should return status when annotation is present", func() {
			fakeRPC := &fakeRPCClient{
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:    "lb-1",
						Ip:    "10.0.0.1",
						State: loadbalancerv1.State_STATE_READY,
					},
				},
			}
			lb.rpcClient = fakeRPC
			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-1"}

			status, exists, err := lb.GetLoadBalancer(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("10.0.0.1"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		It("Should return FIP when active", func() {
			fakeRPC := &fakeRPCClient{
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-2",
						Ip:       "10.0.0.2",
						Fip:      "203.0.113.1",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
						State:    loadbalancerv1.State_STATE_READY,
					},
				},
			}
			lb.rpcClient = fakeRPC
			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-2"}

			status, exists, err := lb.GetLoadBalancer(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
			Expect(status.Ingress[0].IP).To(Equal("203.0.113.1"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		AfterAll(func() {
			ctrl.Finish()
		})
	})

	Context("With ensuring loadbalancer", Ordered, func() {
		var (
			ctrl    *gomock.Controller
			ctx     context.Context
			lb      *loadbalancer
			tenantC *mockclient.MockClient
		)

		BeforeEach(func() {
			ctrl, ctx = gomock.WithContext(context.Background(), GinkgoT())
			tenantC = mockclient.NewMockClient(ctrl)
			c := mockclient.NewMockClient(ctrl)
			lb = &loadbalancer{
				namespace:    "test",
				client:       c,
				tenantClient: tenantC,
				config: LoadBalancerConfig{
					CreationPollInterval: pointer.Int(1),
					CreationPollTimeout:  pointer.Int(5),
					FipNetworkID:        "fip-net",
				},
				rpcClient: nil,
			}
			c.EXPECT().List(ctx, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		})

		It("Should return error if RPC client is not configured", func() {
			svc := newTenantService()
			_, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).To(MatchError(ContainSubstring("RPC client is not configured")))
		})

		It("Should create load balancer and annotate service", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-new",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-new",
						Ip:       "192.168.0.10",
						State:    loadbalancerv1.State_STATE_READY,
						Fip:      "203.0.113.1",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
					},
				},
			}
			lb.rpcClient = fakeRPC

			svc := newTenantService()

			gomock.InOrder(
				// ensureServiceAnnotation for LB ID: Get + Update
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKeyWithValue(LoadBalancerIDAnnotationKey, "lb-new"))
					return nil
				}),
				// ensureServiceAnnotation for create count: Get + Update
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKeyWithValue(LoadBalancerCreateCountAnnotationKey, "1"))
					return nil
				}),
				// ensureServiceAnnotation for listener hash: Get + Update
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKey(LoadBalancerListenerHashAnnotationKey))
					return nil
				}),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("203.0.113.1"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		It("Should fail fast and report the reason when the load balancer reports FAILED", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-doomed",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:    "lb-doomed",
						State: loadbalancerv1.State_STATE_FAILED,
						Error: "no free floating ip in pool",
					},
				},
			}
			lb.rpcClient = fakeRPC

			svc := newTenantService()

			tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				svc.DeepCopyInto(obj.(*corev1.Service))
				return nil
			}).AnyTimes()
			tenantC.EXPECT().Update(ctx, gomock.Any()).Return(nil).AnyTimes()

			start := time.Now()
			_, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			elapsed := time.Since(start)

			Expect(err).To(MatchError(ContainSubstring("no free floating ip in pool")))
			// Must not burn the whole CreationPollTimeout (5s) waiting on a
			// state that never changes.
			Expect(elapsed).To(BeNumerically("<", 3*time.Second))
		})

		It("Should use a fresh idempotency key when recreating a stuck load balancer", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-recreated",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				// The existing LB never leaves PENDING...
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{Id: "lb-stuck", State: loadbalancerv1.State_STATE_PENDING},
				},
				// ...but its replacement comes up healthy.
				getRespAfterCreate: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-recreated",
						Ip:       "192.168.0.30",
						State:    loadbalancerv1.State_STATE_READY,
						Fip:      "203.0.113.9",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
					},
				},
				deleteResp: &loadbalancerv1.DeleteLoadBalancerResponse{Id: "lb-stuck"},
			}
			lb.rpcClient = fakeRPC
			lb.retryCounts = map[string]int{}

			svc := newTenantService()
			svc.Annotations = map[string]string{
				LoadBalancerIDAnnotationKey:          "lb-stuck",
				LoadBalancerCreateCountAnnotationKey: "1",
				// Matching hash, so the retry counter is what advances rather
				// than the "config changed" branch.
				LoadBalancerListenerHashAnnotationKey: listenersHashStr(lb.buildListeners(ctx, svc, []*corev1.Node{}, clusterName)),
			}

			tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
				svc.DeepCopyInto(obj.(*corev1.Service))
				return nil
			}).AnyTimes()
			tenantC.EXPECT().Update(ctx, gomock.Any()).Return(nil).AnyTimes()

			// Drive the retry counter up to the recreate threshold: the LB is
			// stuck in PENDING, so each call increments and errors out.
			for i := 0; i < maxLoadBalancerRetries; i++ {
				_, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
				Expect(err).To(HaveOccurred())
			}
			Expect(fakeRPC.createReqs).To(BeEmpty())

			// Threshold reached: the LB is deleted and recreated.
			_, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeRPC.createReqs).To(HaveLen(1))
			Expect(fakeRPC.createReqs[0].IdempotencyKey).ToNot(Equal(string(svc.UID)))
			_, parseErr := uuid.Parse(fakeRPC.createReqs[0].IdempotencyKey)
			Expect(parseErr).NotTo(HaveOccurred())
		})

		It("Should update load balancer when annotation exists", func() {
			fakeRPC := &fakeRPCClient{
				updateResp: &loadbalancerv1.UpdateLoadBalancerResponse{
					Id:    "lb-existing",
					State: loadbalancerv1.State_STATE_READY,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-existing",
						Ip:       "192.168.0.20",
						State:    loadbalancerv1.State_STATE_READY,
						Fip:      "203.0.113.1",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
					},
				},
			}
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-existing"}

			gomock.InOrder(
				// ensureServiceAnnotation for listener hash: Get
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				// ensureServiceAnnotation for listener hash: Update
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKey(LoadBalancerListenerHashAnnotationKey))
					return nil
				}),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("203.0.113.1"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		It("Should return error if RPC create fails", func() {
			fakeRPC := &fakeRPCClient{
				createErr: errors.New("rpc create failed"),
			}
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			_, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).To(HaveOccurred())
		})

		It("Should create load balancer in internal mode with internal IP as status", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-internal",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:    "lb-internal",
						Ip:    "10.0.0.50",
						State: loadbalancerv1.State_STATE_READY,
					},
				},
			}
			lb.config.IpType = "internal"
			lb.config.FipNetworkID = ""
			lb.rpcClient = fakeRPC

			svc := newTenantService()

			gomock.InOrder(
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("10.0.0.50"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		It("Should create load balancer in both mode with FIP as status and internal IP as annotation", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-both",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-both",
						Ip:       "10.0.0.60",
						State:    loadbalancerv1.State_STATE_READY,
						Fip:      "203.0.113.10",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
					},
				},
			}
			lb.config.IpType = "both"
			lb.config.FipNetworkID = "fip-net"
			lb.rpcClient = fakeRPC

			svc := newTenantService()

			gomock.InOrder(
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKeyWithValue(LoadBalancerIDAnnotationKey, "lb-both"))
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
				// storeInternalIpIfBothMode: Get + Update for internal IP annotation
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKeyWithValue(LoadBalancerInternalIPAnnotationKey, "10.0.0.60"))
					return nil
				}),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("203.0.113.10"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		It("Should skip service when OnlyServiceController is true and annotations are missing", func() {
			fakeRPC := &fakeRPCClient{}
			lb.config.OnlyServiceController = true
			lb.config.FipNetworkID = "fip-net"
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{}

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).To(MatchError(ContainSubstring("implemented by alternate")))
			Expect(status).To(BeNil())
		})

		It("Should return not exists in GetLoadBalancer when OnlyServiceController is true and annotations are missing", func() {
			fakeRPC := &fakeRPCClient{}
			lb.config.OnlyServiceController = true
			lb.config.FipNetworkID = "fip-net"
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{}

			status, exists, err := lb.GetLoadBalancer(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
			Expect(status).To(BeNil())
		})

		It("Should create load balancer with annotation config when OnlyServiceController is true", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-anno",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-anno",
						Ip:       "10.0.0.70",
						State:    loadbalancerv1.State_STATE_READY,
						Fip:      "203.0.113.2",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
					},
				},
			}
			lb.config.OnlyServiceController = true
			lb.config.FipNetworkID = "fip-net"
			lb.config.NetworkID = ""
			lb.config.SubnetID = ""
			lb.config.TenantID = ""
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{
				ServiceNetworkIDAnnotationKey: "anno-net-1",
				ServiceSubnetIDAnnotationKey:  "anno-sub-1",
				ServiceTenantIDAnnotationKey:  "anno-tenant-1",
			}

			gomock.InOrder(
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					updated := obj.(*corev1.Service)
					Expect(updated.Annotations).To(HaveKeyWithValue(LoadBalancerIDAnnotationKey, "lb-anno"))
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					return nil
				}),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("203.0.113.2"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		It("Should set ipMode Proxy and patch KamajiControlPlane in internal mode with OnlyServiceController", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-internal-osc",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:    "lb-internal-osc",
						Ip:    "10.0.0.80",
						State: loadbalancerv1.State_STATE_READY,
					},
				},
			}
			lb.config.OnlyServiceController = true
			lb.config.IpType = "internal"
			lb.config.FipNetworkID = ""
			lb.config.NetworkID = ""
			lb.config.SubnetID = ""
			lb.config.TenantID = ""
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Labels = map[string]string{"kamaji.clastix.io/name": "capi-slowstart-kubevirt"}
			svc.Annotations = map[string]string{
				ServiceNetworkIDAnnotationKey: "anno-net-1",
				ServiceSubnetIDAnnotationKey:  "anno-sub-1",
				ServiceTenantIDAnnotationKey:  "anno-tenant-1",
			}

			gomock.InOrder(
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					Expect(obj.(*corev1.Service).Annotations).To(HaveKeyWithValue(LoadBalancerIDAnnotationKey, "lb-internal-osc"))
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).Return(nil),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).Return(nil),
				tenantC.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("10.0.0.80"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeProxy))
		})

		It("Should patch KamajiControlPlane in external mode with OnlyServiceController", func() {
			fakeRPC := &fakeRPCClient{
				createResp: &loadbalancerv1.CreateLoadBalancerResponse{
					Id:    "lb-ext-osc",
					State: loadbalancerv1.State_STATE_PENDING,
				},
				getResp: &loadbalancerv1.GetLoadBalancerResponse{
					LoadBalancer: &loadbalancerv1.LoadBalancer{
						Id:       "lb-ext-osc",
						Ip:       "10.0.0.90",
						State:    loadbalancerv1.State_STATE_READY,
						Fip:      "203.0.113.5",
						FipState: loadbalancerv1.FipState_FIP_STATE_ACTIVE,
					},
				},
			}
			lb.config.OnlyServiceController = true
			lb.config.IpType = "external"
			lb.config.FipNetworkID = "fip-net"
			lb.config.NetworkID = ""
			lb.config.SubnetID = ""
			lb.config.TenantID = ""
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Labels = map[string]string{"kamaji.clastix.io/name": "capi-slowstart-kubevirt"}
			svc.Annotations = map[string]string{
				ServiceNetworkIDAnnotationKey: "anno-net-2",
				ServiceSubnetIDAnnotationKey:  "anno-sub-2",
				ServiceTenantIDAnnotationKey:  "anno-tenant-2",
			}

			gomock.InOrder(
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					Expect(obj.(*corev1.Service).Annotations).To(HaveKeyWithValue(LoadBalancerIDAnnotationKey, "lb-ext-osc"))
					return nil
				}),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).Return(nil),
				tenantC.EXPECT().Get(ctx, client.ObjectKey{Name: "service1", Namespace: "test"}, gomock.Any()).DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					svc.DeepCopyInto(obj.(*corev1.Service))
					return nil
				}),
				tenantC.EXPECT().Update(ctx, gomock.Any()).Return(nil),
				tenantC.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
			)

			status, err := lb.EnsureLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
			Expect(status).ToNot(BeNil())
			Expect(status.Ingress).To(HaveLen(1))
			Expect(status.Ingress[0].IP).To(Equal("203.0.113.5"))
			Expect(status.Ingress[0].IPMode).ToNot(BeNil())
			Expect(*status.Ingress[0].IPMode).To(Equal(corev1.LoadBalancerIPModeVIP))
		})

		AfterEach(func() {
			ctrl.Finish()
		})
	})

	Context("With updating loadbalancer", Ordered, func() {
		var (
			ctrl    *gomock.Controller
			ctx     context.Context
			lb      *loadbalancer
			tenantC *mockclient.MockClient
		)

		BeforeEach(func() {
			ctrl, ctx = gomock.WithContext(context.Background(), GinkgoT())
			tenantC = mockclient.NewMockClient(ctrl)
			c := mockclient.NewMockClient(ctrl)
			lb = &loadbalancer{
				namespace:    "test",
				client:       c,
				tenantClient: tenantC,
				config: LoadBalancerConfig{
					CreationPollInterval: pointer.Int(1),
					CreationPollTimeout:  pointer.Int(5),
					FipNetworkID:        "fip-net",
				},
				rpcClient: nil,
			}
			c.EXPECT().List(ctx, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		})

		It("Should return error if RPC client is nil", func() {
			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-1"}
			err := lb.UpdateLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).To(MatchError(ContainSubstring("RPC client is not configured")))
		})

		It("Should return error if annotation is missing", func() {
			fakeRPC := &fakeRPCClient{}
			lb.rpcClient = fakeRPC
			svc := newTenantService()
			err := lb.UpdateLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).To(MatchError(ContainSubstring("load balancer ID not found")))
		})

		It("Should update load balancer via RPC", func() {
			fakeRPC := &fakeRPCClient{updateResp: &loadbalancerv1.UpdateLoadBalancerResponse{}}
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-1"}

			err := lb.UpdateLoadBalancer(ctx, clusterName, svc, []*corev1.Node{})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			ctrl.Finish()
		})
	})

	Context("With ensuring loadbalancer is deleted", Ordered, func() {
		var (
			ctrl    *gomock.Controller
			ctx     context.Context
			lb      *loadbalancer
			tenantC *mockclient.MockClient
		)

		BeforeEach(func() {
			ctrl, ctx = gomock.WithContext(context.Background(), GinkgoT())
			tenantC = mockclient.NewMockClient(ctrl)
			lb = newTestLoadBalancerWithTenantClient(ctrl, tenantC, nil)
		})

		It("Should delete load balancer by annotation ID", func() {
			fakeRPC := &fakeRPCClient{deleteResp: &loadbalancerv1.DeleteLoadBalancerResponse{}}
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-1"}

			err := lb.EnsureLoadBalancerDeleted(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should handle RPC not found as success", func() {
			fakeRPC := &fakeRPCClient{deleteErr: status.Error(codes.NotFound, "not found")}
			lb.rpcClient = fakeRPC

			svc := newTenantService()
			svc.Annotations = map[string]string{LoadBalancerIDAnnotationKey: "lb-1"}

			err := lb.EnsureLoadBalancerDeleted(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should find LB by name and delete if annotation is missing", func() {
			fakeRPC := &fakeRPCClient{
				listResp: &loadbalancerv1.ListLoadBalancersResponse{
					LoadBalancers: []*loadbalancerv1.LoadBalancerSummary{
						{Name: lbServiceName, Id: "lb-found"},
					},
				},
				deleteResp: &loadbalancerv1.DeleteLoadBalancerResponse{},
			}
			lb.rpcClient = fakeRPC

			svc := newTenantService()

			err := lb.EnsureLoadBalancerDeleted(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should succeed when load balancer not found", func() {
			fakeRPC := &fakeRPCClient{}
			lb.rpcClient = fakeRPC

			svc := newTenantService()

			err := lb.EnsureLoadBalancerDeleted(ctx, clusterName, svc)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			ctrl.Finish()
		})
	})

	Context("With deriving create idempotency keys", func() {
		svc := newTenantService()

		It("Should reuse the service UID for the first attempt", func() {
			Expect(createIdempotencyKey(svc, 0)).To(Equal(string(svc.UID)))
		})

		It("Should be stable for a given attempt", func() {
			Expect(createIdempotencyKey(svc, 2)).To(Equal(createIdempotencyKey(svc, 2)))
		})

		It("Should differ between attempts and stay a valid UUID", func() {
			keys := map[string]bool{}
			for attempt := 0; attempt <= 3; attempt++ {
				key := createIdempotencyKey(svc, attempt)
				_, err := uuid.Parse(key)
				Expect(err).NotTo(HaveOccurred())
				Expect(keys).NotTo(HaveKey(key))
				keys[key] = true
			}
		})

		It("Should still produce a UUID when the service UID is not one", func() {
			odd := newTenantService()
			odd.UID = types.UID("not-a-uuid")
			_, err := uuid.Parse(createIdempotencyKey(odd, 1))
			Expect(err).NotTo(HaveOccurred())
		})
	})

})

# Developer guide

## 1. Where the code lives

```
cmd/kubevirt-cloud-controller-manager/
  main.go              standard cloud-controller-manager entrypoint
  kubevirteps.go       optional EndpointSlice controller (off unless enableEPSController)
pkg/provider/
  cloud.go             CloudConfig / LoadBalancerConfig, provider factory, client wiring
  loadbalancer.go      the whole LoadBalancer implementation (this fork's core)
  loadbalancer_test.go ginkgo suite
  instances_v2.go      node metadata from VirtualMachineInstances
  mock/client/         gomock mock of controller-runtime client.Client
pkg/rpc/loadbalancer/
  client.go            retrying/authenticating gRPC wrapper + error translation
  gen/                 loadbalancer.proto and the protoc-generated Go code
charts/kubevirt-cloud-controller-manager/
plan/                  scratch: original requirements + upstream proto snapshots
yaml/                  sample workloads (tcp.yaml, http.yaml)
design.md              the original implementation plan (historical)
```

The upstream `Instances`, `Zones`, `Clusters` and `Routes` interfaces are all
unimplemented; `InstancesV2` and `LoadBalancer` are the live ones.

## 2. Build and test

```bash
go build ./...
go test ./pkg/provider/...          # ginkgo suite, no cluster required
go vet ./...
gofmt -l pkg cmd                    # some files in this branch are not gofmt-clean
```

Container image / release targets live in the `Makefile` (upstream, unchanged).

> A build of `kubevirt-cloud-controller-manager` lands at the repo root. It was
> committed once by accident and has since been removed from git — add it to
> `.gitignore` so that does not recur.

## 3. Regenerating the protobuf code

The proto is a vendored copy of the LoadBalancer API's
`loadbalancer/v1/loadbalancer.proto` (`plan/new.proto` is the upstream snapshot;
`pkg/rpc/loadbalancer/gen/loadbalancer.proto` is what was actually generated
from). It imports `buf/validate/validate.proto`, so that dependency must be on
the include path.

The checked-in code was produced with protoc v6.30.0 / protoc-gen-go v1.36.11,
overriding the upstream `go_package` so the generated package resolves inside
this module:

```bash
protoc \
  -I . -I "$(buf_validate_include_dir)" \
  --go_out=. --go_opt=module=kubevirt.io/cloud-provider-kubevirt \
  --go_opt=Mpkg/rpc/loadbalancer/gen/loadbalancer.proto=kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer/gen \
  --go-grpc_out=. --go-grpc_opt=module=kubevirt.io/cloud-provider-kubevirt \
  --go-grpc_opt=Mpkg/rpc/loadbalancer/gen/loadbalancer.proto=kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer/gen \
  pkg/rpc/loadbalancer/gen/loadbalancer.proto
```

The `buf.validate` options are **not** enforced client-side — they are
documentation here and are evaluated by the server. Read them anyway when
building requests; several are easy to trip (see §7).

## 4. Configuration plumbing

Adding a knob means touching four places, in this order:

1. `pkg/provider/cloud.go` → a field on `LoadBalancerConfig` with a `yaml:` tag.
2. `charts/.../templates/configmap.yaml` → emit it into the `cloud-config` blob.
3. `charts/.../values.yaml` → a default plus a comment.
4. `docs/user-guide.md` → the values table.

`createDefaultCloudConfig()` supplies defaults for anything absent from the
ConfigMap; the YAML is unmarshalled over those defaults.

## 5. The `rpcLBClient` seam

`loadbalancer.rpcClient` is typed as the local `rpcLBClient` interface (five
methods: Create/Get/Update/Delete/List), not as `*rpc.Client`. That is the test
seam — `loadbalancer_test.go` injects a `fakeRPCClient` struct with canned
responses. If you start calling `AllocateFloatingIp` / `ReleaseFloatingIp` from
the provider, add them to the interface *and* to the fake.

The Kubernetes clients are mocked with gomock:

```bash
go generate ./pkg/provider/mock/client   # regenerates mocks.go via mockgen
```

Helpers `newTestLoadBalancer` (both clients mocked) and
`newTestLoadBalancerWithTenantClient` (real fake client for the tenant side, used
by the annotation-write and Kamaji-patch tests) are the entry points for new
tests.

## 6. Extension points

**A different protocol per port.** Today `buildListeners` decides TCP vs HTTP
once, from the presence of `kubevirt.io/http-path`, and applies it to every port.
Per-port control would mean keying annotations by `ServicePort.Name`
(e.g. `kubevirt.io/http-path.<port-name>`) and reading them inside the port loop.

**Tunable health monitors.** The `defaultHealthMonitor*` constants are compiled
in. Expose them as annotations or config if needed — but keep
`timeout < interval`, which the server enforces.

**UDP listeners.** The proto has `PROTOCOL_UDP`; `buildListeners` never emits it.
`ServicePort.Protocol` is currently ignored.

**Backend selection.** `buildBackendRefs` uses node internal IP + NodePort, with
an external-IP fallback and then a VMI-label fallback. `ExternalTrafficPolicy:
Local` is not honoured — all nodes are used as backends regardless.

**Security groups / QoS.** Hard-coded to `security_group_disabled: true` and
`qos_policy_disabled: true` on both create and update. Both have `*_id` fields in
the proto if you want to wire them to config.

## 7. Gotchas

* **`idempotency_key` must be a UUID**, and must change per create *attempt*.
  `createIdempotencyKey` handles both: attempt 0 is the Service UID, later
  attempts are v5 UUIDs derived from it. Do not collapse it back to the bare UID
  — the recreate path would replay the deleted creation instead of making a new
  load balancer.
* **`tenant_id`, `network_id`, `subnet_id` must be UUID-shaped** (the server's
  CEL rule allows the dashless form too). The tenant fallback to
  `service.Namespace` therefore only works when namespaces are named after the
  OpenStack project ID.
* **A listener needs ≥ 1 backend.** Create/update is rejected outright when the
  node list is empty and the VMI fallback finds nothing.
* **HTTP listeners must carry a full health monitor** — path starting with `/`,
  a non-zero method, and a non-empty `expected_status_codes`. `buildListeners`
  satisfies this; keep it that way if you refactor.
* **`http_route` is only valid on HTTP listeners.**
* **`UpdateLoadBalancer` replaces the listener list wholesale.** Always send the
  complete desired set.
* **The retry counter (`retryCounts`) is in-memory** and keyed by Service UID; it
  resets on restart. The recreate counter is an annotation, so it survives.
* **`EnsureLoadBalancer` blocks** for up to `creationPollTimeout` while polling.
  With `concurrentServiceSyncs: 1` that serialises all LB provisioning.
* **Every RPC attempt needs a deadline.** `retryOnFailure` wraps each attempt in
  `Config.Timeout`; keep it that way. Calls pass `grpc.WaitForReady(true)`, so a
  call made on a bare caller context never returns while the server is down.
* **Several `ensureServiceAnnotation` calls ignore their error** (create-count,
  listener-hash, internal-ip). Only the `loadbalancer-id` write is treated as
  fatal — which is the right priority, but it means the hash can drift and cause
  a redundant update on the next sync.
* **`pendingStatus()` is currently unused** — provisioning is synchronous, so
  nothing publishes a `pending` hostname. Remove it or use it if you make the
  path asynchronous.
* **Transport is plaintext.** `cloud.go` dials with
  `insecure.NewCredentials()`; the `apiKey` bearer token rides on that. Adding
  TLS means changing `DialOpts` and flipping `apiKeyCreds.RequireTransportSecurity`.

## 8. Debugging against a live cluster

```bash
# CCM logs (raise verbosity with -v=4 on the container args)
kubectl -n <ns> logs deploy/kubevirt-cloud-controller-manager -f

# what the CCM decided about a Service
kubectl get svc <name> -o jsonpath='{.metadata.annotations}{"\n"}{.status}' | jq

# talk to the LoadBalancer API directly
grpcurl -plaintext -H 'authorization: Bearer <apiKey>' \
  10.2.3.250:9000 loadbalancer.v1.LoadBalancerService/ListLoadBalancers

# render the chart without installing
helm template ccm ./charts/kubevirt-cloud-controller-manager \
  --set onlyServiceController=true | less
```

To exercise the provider locally without an infra cluster, run the ginkgo suite
with a focus:

```bash
go test ./pkg/provider/ -run TestProvider -args -ginkgo.focus="KamajiControlPlane"
```

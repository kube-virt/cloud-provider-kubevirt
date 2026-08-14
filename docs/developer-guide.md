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

`pkg/rpc/loadbalancer/gen/loadbalancer.proto` is a verbatim copy of the
LoadBalancer API's `proto/loadbalancer/v1/loadbalancer.proto`. Refresh it and the
generated code together:

```bash
hack/update-proto.sh [path/to/loadbalancer.proto]   # defaults to ../loadbalancer-api/...
```

The script copies the proto in and runs `buf` with the local `protoc-gen-go` /
`protoc-gen-go-grpc` plugins, overriding the upstream `go_package` so the
generated package resolves inside this module. Notes on why it looks the way it
does:

* The proto has to stay at `pkg/rpc/loadbalancer/gen/loadbalancer.proto` inside
  the buf module. That path is baked into the generated `source:` header and into
  the global proto registry key, so moving it would be a silent behaviour change.
* `buf` runs in a throwaway workspace so no `buf.yaml` has to live in this repo.
  The workspace carries a `buf.lock` pinning `buf.build/bufbuild/protovalidate`,
  which makes generation work offline once that module has been fetched once.
* The generated file's blank import of the protovalidate Go bindings is stripped.
  Nothing here reads the options at runtime, they survive in the raw descriptor
  regardless, and keeping the import would add a module dependency purely so an
  unused extension can register itself.

After regenerating, check `git diff --stat pkg/rpc/loadbalancer/gen/` — only the
proto and the two `.pb.go` files should move.

The `buf.validate` options are **not** enforced client-side — they are
documentation here and are evaluated by the server's protovalidate interceptor,
*before* the handler and its Go-side defaults run. Read them when building
requests; several are easy to trip (see §7).

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

**A different HTTP route per port.** Today `buildListeners` decides TCP vs HTTP
once, from the presence of `kubevirt.io/http-path`, and applies it to every TCP
port. Per-port control would mean keying annotations by `ServicePort.Name`
(e.g. `kubevirt.io/http-path.<port-name>`) and reading them inside the port loop.

**Several rules per HTTP listener.** `ListenerRule` supports up to 16 rules per
HTTP listener, each with its own matches, backends and health monitor; the CCM
emits exactly one. Exposing that would mean an annotation format rich enough to
carry a rule list, plus extending `mergeListenerIDs` — which currently matches
rules and backends by position, valid only because there is exactly one of each.

**Weighted backends.** A rule may carry up to 16 `RuleBackend`s with independent
weights (a canary split, say). The CCM emits one at weight 1. Note that weight 0
is only honoured on a backend that already has an id; on a new one the server
rewrites it to 1, and a rule whose backends are all 0 is rejected outright.

**Hostname routing.** `Listener.hostnames` scopes a listener to specific HTTP
`Host` values. No annotation feeds it today, so it is always empty (= any host).

**Tunable health monitors.** The `defaultHealthMonitor*` constants are compiled
in. Expose them as annotations or config if needed — but keep
`timeout < interval`, which the server enforces.

**Backend selection.** `buildEndpoints` uses node internal IP + NodePort, with
an external-IP fallback and then a VMI-label fallback (the VMI's `default`
interface only — the others belong to the CNI running inside the guest).
Addresses are filtered through `isUsableNodeIP`. `ExternalTrafficPolicy: Local`
is not honoured — all nodes are used as backends regardless.

**Unused RPCs.** The generated client also carries `CreateListener` /
`ModifyListener` / `CreateRule` / `ModifyRule` / `SetRulePriorities`,
`BatchDeleteLoadBalancers`, `AllocateFloatingIp` / `ReleaseFloatingIp` and
`Ping` / `AuthenticatedPing`. None are called. The rule-level RPCs would let an
update touch one rule instead of resending every listener; `AuthenticatedPing`
would let the CCM verify its API key at startup (check `auth_enforced` on the
response — against a server with no API key configured it succeeds for any
token).

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
* **Endpoint IPs are validated more strictly downstream than by the API.** The
  proto only asks for `string.ip`, which accepts a link-local address; the
  LoadBalancer controller renders each endpoint into an Envoy Gateway `Backend`,
  whose CRD pattern for `endpoints[].ip.address` does not match `fe80::…`. The
  rejected `Backend` leaves the `LoadBalancerClaim` un-`Ready`, and the load
  balancer fails 15 minutes later with `Provisioning timed out` — one unusable
  endpoint kills the whole LB, not just that member. Hence `isUsableNodeIP`, on
  both the node-address and the endpoint path.
* **A listener needs ≥ 1 rule, a rule ≥ 1 backend, a backend ≥ 1 endpoint.**
  `validateListeners` fails the reconcile with a readable reason when the node
  list is empty and the VMI fallback finds nothing, rather than letting the server
  reject it.
* **`weight` must be ≥ 1 on a new backend.** The `rule-weight-sum` CEL rule runs
  in the server's validation interceptor, before the handler's "unset means 1"
  default, so a lone backend at weight 0 is a hard `InvalidArgument`.
* **HTTP listeners must carry a full health monitor** — path starting with `/`,
  a non-zero method, and a non-empty `expected_status_codes`. `buildListeners`
  satisfies this; keep it that way if you refactor.
* **Matches are HTTP-only, and an empty match is not the same as no matches.**
  TCP/UDP listeners must carry exactly one rule with no matches; on HTTP, an
  empty `matches` list is the catch-all while an `HttpRouteMatch` that constrains
  nothing is rejected.
* **`UpdateLoadBalancer` replaces the listener list wholesale and reconciles by
  id.** Always send the complete desired set, and graft the ids of the current
  listeners onto it (`mergeListenerIDs`) — anything sent without a known id is
  recreated from scratch, taking its generated data plane objects with it. Note
  that the update is `AllCols()` server-side: a field you omit from a listener is
  reset to its zero value.
* **Sending an empty listener list is a no-op, not a delete.** The server skips
  the whole listener block when `len(listeners) == 0`.
* **`ListLoadBalancers` rejects `page_size` below 1**, so the delete-by-name
  fallback has to set it explicitly and follow `next_page_token`.
* **The retry counter (`retryCounts`) is in-memory** and keyed by Service UID; it
  resets on restart. The recreate counter is an annotation, so it survives.
* **`EnsureLoadBalancer` blocks** for up to `creationPollTimeout` while polling.
  With `concurrentServiceSyncs: 1` that serialises all LB provisioning.
* **Every RPC attempt needs a deadline.** `retryOnFailure` wraps each attempt in
  `Config.Timeout`; keep it that way. Calls pass `grpc.WaitForReady(true)`, so a
  call made on a bare caller context never returns while the server is down.
* **Only the `loadbalancer-id` annotation write is fatal.** The others
  (create-count, listener-hash, internal-ip) are logged and carried on from,
  which is the right priority — but a hash that never lands makes every
  subsequent sync look like a config change.
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

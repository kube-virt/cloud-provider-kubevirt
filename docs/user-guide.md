# User guide

How to deploy this CCM and how to ask it for a load balancer. Read
[architecture.md](architecture.md) first if the two deployment modes are new to
you.

## 1. Prerequisites

* A Harvester/KubeVirt **infra cluster** where the CCM will run.
* A reachable **LoadBalancer API** gRPC endpoint (`host:port`, plaintext).
* OpenStack IDs: network, subnet, external (FIP) network, and project/tenant.
* A **kubeconfig Secret** in the release namespace named
  `<clusterName>-kubeconfig`, with the kubeconfig under the key `value`. This is
  the cluster whose Services the CCM will manage:
  * mode 1 → the tenant cluster's admin kubeconfig (CAPI already creates
    `<cluster>-kubeconfig` in this exact shape);
  * mode 2 → a kubeconfig for the **infra** cluster itself.

## 2. Install

```bash
helm install ccm-tenant ./charts/kubevirt-cloud-controller-manager \
  --namespace new3 --create-namespace \
  --set clusterName=capi-slowstart \
  --set rpcServerAddr=10.2.3.250:9000 \
  --set networkID=e31344bd-5840-4e67-b9f4-42f2d022655c \
  --set subnetID=34ae98d5-c02d-48f7-a525-19dc6ea10032 \
  --set fipNetworkID=46a8ddf9-a531-4c81-9451-d221f59b19ea \
  --set tenantID=8743f99e2933433e8cc1b760de8d63f0 \
  --set ipType=external
```

For the Kamaji control-plane instance (mode 2), in the same infra cluster:

```bash
helm install ccm-infra ./charts/kubevirt-cloud-controller-manager \
  --namespace kube-system \
  --set clusterName=infra \
  --set onlyServiceController=true \
  --set concurrentServiceSyncs=5 \
  --set rpcServerAddr=10.2.3.250:9000 \
  --set fipNetworkID=46a8ddf9-a531-4c81-9451-d221f59b19ea \
  --set ipType=internal
```

In mode 2, `networkID` / `subnetID` / `tenantID` from values are ignored — every
Service supplies its own (see §4). `fipNetworkID` is still read from values.

The chart installs a ServiceAccount, a ConfigMap (`cloud-config`), a Deployment,
and either a namespaced `Role`/`RoleBinding` (mode 1) or a
`ClusterRole`/`ClusterRoleBinding` including `kamajicontrolplanes` (mode 2).

## 3. Chart values

| Value | Default | Meaning |
|-------|---------|---------|
| `image.repository` / `image.tag` / `image.pullPolicy` | `sumon124816/openstack` / `ccm` / `Always` | CCM image |
| `resources` | 100m / 128Mi requests | Container resources |
| `clusterName` | `capi-slowstart` | Passed as `--cluster-name`; also names the kubeconfig Secret (`<clusterName>-kubeconfig`) and is the `cluster.x-k8s.io/cluster-name` value used for the VMI backend fallback |
| `rpcServerAddr` | `10.2.3.250:9000` | LoadBalancer API gRPC address. Either `host:port` or `http://host:port` — the scheme is stripped before dialing. The transport is plaintext, so `https://` is rejected at startup rather than silently downgraded |
| `rpcKeepAlive` | `30` | Deadline for a single RPC attempt, seconds. Each retry gets a fresh one, so an unreachable server blocks a sync for at most `(rpcRetryMax + 1) x rpcKeepAlive` |
| `rpcRetryMax` | `3` | Extra retry attempts per RPC |
| `apiKey` | `""` | Bearer token for the LoadBalancer API; empty disables auth |
| `networkID`, `subnetID`, `tenantID` | — | OpenStack IDs used in mode 1 (ignored in mode 2) |
| `fipNetworkID` | — | External network for floating IPs. **Required whenever `ipType` is `external` or `both`** |
| `ipType` | `external` | `external` \| `internal` \| `both`; default for all Services |
| `onlyServiceController` | `false` | Mode 2: disable node/route controllers, read network config from Service annotations, enable the Kamaji patch and `ipMode: Proxy` |
| `concurrentServiceSyncs` | `1` | `--concurrent-service-syncs`; raise it to provision several LBs in parallel |
| `cloudConfig.loadBalancer.creationPollInterval` | `5` | Seconds between readiness polls |
| `cloudConfig.loadBalancer.creationPollTimeout` | `120` | Seconds to wait for a LB to become READY |
| `cloudConfig.instancesV2.enabled` | `true` | Node metadata from VMIs (leave on for mode 1) |
| `cloudConfig.instancesV2.zoneAndRegionEnabled` | `false` | Add zone/region labels |
| `serviceAccount.create` / `serviceAccount.name` | `true` / `cloud-controller-manager` | ServiceAccount |

## 4. Service annotations

### Everywhere

| Annotation | Values | Effect |
|------------|--------|--------|
| `kubevirt.io/http-path` | e.g. `/healthz` | Makes every eligible listener of this Service HTTP; sets the rule's path match (prefix) and the health-check path |
| `kubevirt.io/http-method` | `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS` | The rule's method match and the health-check method. Only meaningful together with `http-path`; unknown values fall back to `GET` |

### Only when `onlyServiceController: true`

| Annotation | Required | Effect |
|------------|----------|--------|
| `loadbalancer.kubevirt.io/network-id` | **yes** | OpenStack network UUID. Missing → the Service is ignored entirely |
| `loadbalancer.kubevirt.io/subnet-id` | **yes** | OpenStack subnet UUID. Missing → the Service is ignored entirely |
| `loadbalancer.kubevirt.io/tenant-id` | no | OpenStack project UUID; defaults to the Service's namespace |
| `loadbalancer.kubevirt.io/ip-type` | no | `external` \| `internal` \| `both`; overrides the chart's `ipType` per Service |

### Written by the CCM (do not edit)

| Annotation | Meaning |
|------------|---------|
| `kubevirt.io/loadbalancer-id` | LB UUID. Deleting it orphans the load balancer and makes the CCM create a second one |
| `kubevirt.io/loadbalancer-listener-hash` | Change detector for the listener set |
| `kubevirt.io/loadbalancer-create-count` | Recreate attempts (hard stop at 3) |
| `kubevirt.io/loadbalancer-internal-ip` | Internal VIP, only for `ip-type: both` |

### Label consumed by the CCM

`kamaji.clastix.io/name: <kamajicontrolplane-name>` — in mode 2, the CCM patches
that `KamajiControlPlane` in the Service's namespace with
`spec.network.advertiseAddress = <internal VIP>` once the LB is ready.

## 5. Examples

### TCP

```yaml
apiVersion: v1
kind: Service
metadata:
  name: test-tcp-lb
spec:
  type: LoadBalancer
  selector:
    app: test-tcp
  ports:
    - protocol: TCP
      port: 9000        # LB listener port
      targetPort: 8080  # container port
```

One TCP listener on 9000, backends = every node internal IP on the NodePort
Kubernetes allocated for this port, TCP health monitor with the defaults.

### HTTP

```yaml
apiVersion: v1
kind: Service
metadata:
  name: test-multi-lb
  annotations:
    kubevirt.io/http-path: "/"
    kubevirt.io/http-method: "GET"
spec:
  type: LoadBalancer
  selector:
    app: multi-container-test
  ports:
    - name: port-json
      protocol: TCP
      port: 8070
      targetPort: 8080
```

HTTP listener with an HTTPRoute prefix match on `/` and an HTTP health monitor
(`GET /`, expect `200`).

### Kamaji control-plane Service (mode 2)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: new3-kubeapiserver
  namespace: new3
  labels:
    kamaji.clastix.io/name: new3
  annotations:
    loadbalancer.kubevirt.io/network-id: e31344bd-5840-4e67-b9f4-42f2d022655c
    loadbalancer.kubevirt.io/subnet-id: 34ae98d5-c02d-48f7-a525-19dc6ea10032
    loadbalancer.kubevirt.io/tenant-id: 8743f99e2933433e8cc1b760de8d63f0
    loadbalancer.kubevirt.io/ip-type: internal
spec:
  type: LoadBalancer
  ports:
    - port: 6443
      targetPort: 6443
```

Result: an internal VIP published with `ipMode: Proxy`, and
`KamajiControlPlane/new3` patched so tenant kubelets dial that VIP instead of a
floating IP.

## 6. Checking status

```bash
kubectl get svc test-tcp-lb -o wide
kubectl get svc test-tcp-lb -o jsonpath='{.metadata.annotations}' | jq
kubectl describe svc test-tcp-lb          # events carry the CCM's error messages
kubectl -n <ns> logs deploy/kubevirt-cloud-controller-manager -f
```

While provisioning, `EXTERNAL-IP` stays `<pending>`; the CCM blocks inside
`EnsureLoadBalancer` polling the API until the LB is `READY` (and the FIP is
`ACTIVE`, if requested).

## 7. Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `EXTERNAL-IP` stuck `<pending>`, no annotations at all | Mode 2 and the Service is missing `network-id`/`subnet-id` → it is deliberately skipped. Otherwise: no RPC client (empty `rpcServerAddr`) |
| Event: `FIP network ID is not configured…` | `ipType` is `external`/`both` but `fipNetworkID` is empty |
| Event: `Invalid load balancer configuration` | The API rejected the spec. Common causes: `tenant_id`/`network_id`/`subnet_id` not UUID-shaped (a namespace name used as tenant fallback is **not** a UUID); a NodePort of 0 because `allocateLoadBalancerNodePorts: false`; HTTP listener whose health-check path does not start with `/` |
| Event: `no backend endpoints available for port <n>` | No node in the watched cluster reports an address, and the VMI fallback found nothing either. Check that nodes are Ready and that the VMIs carry the `cluster.x-k8s.io/role=worker` and `cluster.x-k8s.io/cluster-name` labels |
| `load balancer <id> failed (state STATE_FAILED: Provisioning timed out …)` and the LB's backend lists an `fe80::…` endpoint | An unroutable address reached the backend list. The datapath rejects it, so the load balancer never finishes provisioning. Check `kubectl get node -o jsonpath='{.items[*].status.addresses}'` — a link-local there means a CCM older than this fix is populating node addresses |
| `EXTERNAL-IP` is set but the CCM logs `SyncLoadBalancerFailed` | Another IPAM owns `status.loadBalancer.ingress`. With Cilium, check `kubectl get svc <name> -o jsonpath='{.status.conditions}'` for `cilium.io/IPAMRequestSatisfied`; if present, delete the `CiliumLoadBalancerIPPool` or set `enable-lb-ipam=false` in the CNI values so only the CCM publishes an address |
| Event: `Load balancer service temporarily unavailable` | The gRPC endpoint is down or unreachable; the client already retried `rpcRetryMax` times |
| Event: `Permission denied…` | Wrong or missing `apiKey` |
| `load balancer <id> not ready within 2m0s` | Provisioning slower than `creationPollTimeout`. Raise the timeout, or check the LB API side |
| `load balancer <id> failed (state STATE_FAILED: …)` | The LB API gave up; the text after the colon is its own `error` field. The CCM stops waiting immediately and retries, recreating the LB after 10 unsuccessful syncs |
| `load balancer <id> failed to provision after 3 create attempts` | Hard stop. Fix the underlying problem, then delete the `kubevirt.io/loadbalancer-create-count` and `kubevirt.io/loadbalancer-id` annotations to start over — and delete the orphaned LB via the API |
| Kamaji `advertiseAddress` never updated | The Service lacks the `kamaji.clastix.io/name` label, `onlyServiceController` is false, or the ClusterRole lacks `kamajicontrolplanes` patch rights |
| Traffic to an internal VIP lands on the wrong cluster | Confirm the status shows `ipMode: Proxy`; with `VIP` kube-proxy short-circuits the address locally |
| Deleting the Service leaves an LB behind | The `loadbalancer-id` annotation was lost *and* the LB name no longer matches `a<service-uid-without-dashes>`; delete it via the LoadBalancer API |

## 8. Operational notes

* **Deleting a Service deletes the load balancer**, including its floating IP.
* **Do not hand-edit the CCM annotations.** They are the only record linking a
  Service to its load balancer.
* **Node changes** (scale up/down, IP change) rewrite every listener's endpoint
  list on the next sync. The listener, its rule and its backend keep their
  identifiers, so only the endpoints move; other node events, which do not change
  the endpoint list, no longer send an update at all.
* **Rolling the CCM** loses the in-memory "not ready" retry counter, so a stuck
  LB gets another 10 retries before the recreate path triggers. The recreate
  count survives, because it is an annotation.

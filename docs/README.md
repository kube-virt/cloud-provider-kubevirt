# Documentation

This directory documents the fork of `cloud-provider-kubevirt` whose LoadBalancer
implementation has been rewritten to talk to an external gRPC **LoadBalancer API**
(OpenStack-backed) instead of creating in-cluster KubeVirt/kube-vip Services.

| Document | Audience | Contents |
|----------|----------|----------|
| [architecture.md](architecture.md) | Everyone | Cluster topology, the two CCM deployment modes, reconciliation flow, IP/FIP model, why `ipMode: Proxy` and the Kamaji `advertiseAddress` patch exist |
| [user-guide.md](user-guide.md) | Cluster operators / app developers | Installing the Helm chart, every value and Service annotation, worked examples, troubleshooting |
| [developer-guide.md](developer-guide.md) | Contributors | Code layout, proto regeneration, build/test, how to extend the provider, known limitations |

## TL;DR

* Tenant clusters have a **Kamaji** control plane (pods on the Harvester infra
  cluster) and **KubeVirt VM** worker nodes on that same infra cluster.
* The CCM runs **on the infra cluster** and is deployed **twice**:
  1. once pointed at the *tenant* API server, to serve `type: LoadBalancer`
     Services created by tenant users;
  2. once pointed at the *infra* API server with `onlyServiceController: true`,
     to give the Kamaji control-plane Services (`kube-apiserver`, `konnectivity`)
     a real load balancer IP.
* Every LB create/update/delete becomes a gRPC call to the LoadBalancer API. No
  Kubernetes Service, kube-vip pod or EndpointSlice is created by the LB path.

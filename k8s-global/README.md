# TicketMEI on the global (UMa) cluster

Manifests for the shared cluster (namespace `grupo01`, Argo CD at http://argocd.10.2.15.161.nip.io).
The local cluster (Docker Desktop) keeps using `k8s/`. Access needs the UMa network or the VPN.

## What differs from `k8s/`

| Topic | Local (`k8s/`) | Global (`k8s-global/`) |
|---|---|---|
| Namespace | `ticketmei`, created by Argo CD | `grupo01`, already exists (set once in `kustomization.yaml`) |
| Pod security | none | PSA *restricted*: non-root, no privilege escalation, all capabilities dropped, seccomp RuntimeDefault |
| RabbitMQ | own pod | cluster RabbitMQ, vhost `/grupo01`, credentials from Secret `grupo01-user-user-credentials` |
| Prometheus / Grafana | own pods | cluster ones: one `ServiceMonitor` per service (+ `NetworkPolicy`) |
| Gateway | `LoadBalancer` | `ClusterIP` + `Ingress` (`ticketmei-grupo01.10.2.15.161.nip.io`) |

The services read `RABBITMQ_URL`; here it is built from the Secret with `$(RABBITMQ_USER)` / `$(RABBITMQ_PASSWORD)`.
Passwords with special characters (`@ : /`) would need URL-encoding.

## Changing the group number

Everything group-specific is the string `grupo01`: `kustomization.yaml`, the Secret name and vhost in
`order/` and `notification/`, the Ingress host, and `argocd/application-global.yaml`.

## Deploy

1. Push to `parte5`; the pipeline builds the images and writes the new tags in `k8s/` **and** `k8s-global/`.
2. The GHCR packages must be **public** (otherwise `ImagePullBackOff`).
3. Argo CD UI -> New App -> Edit as YAML -> paste `argocd/application-global.yaml`.
4. Check: all pods `Running`; `http://ticketmei-grupo01.10.2.15.161.nip.io/events`.

## Dashboards

`grafana-dashboards/*.json` are the project dashboards with a datasource variable. In
http://grafana.10.2.15.161.nip.io: Dashboards -> New -> Import -> upload the JSON -> choose the group
folder and the Prometheus datasource. The ServiceMonitors scrape every 15 s so that the `rate(...[30s])` panels work.

## Saga load test

`extras/k6-saga-job.yaml` (needs `kubectl` access to the namespace, see the header of the file).

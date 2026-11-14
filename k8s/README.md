# Kubernetes (Docker Desktop) + Argo CD

Kustomize entry point: `k8s/kustomization.yaml` (namespace `ticketmei`).

> The config files used by the ConfigMaps (`prometheus/prometheus.yml`,
> `rabbitmq/enabled_plugins`, `grafana/provisioning/**`, `grafana/dashboards/*.json`)
> are **copies** of the ones in the repository root, because Kustomize can only
> read files inside `k8s/`. When you change one, change both.

## 1. Run it by hand (to check the manifests)

Docker Desktop -> Settings -> Kubernetes -> Enable (cluster type: kubeadm), then:

```powershell
kubectl get nodes

# local images with the tag the manifests use (:dev)
foreach ($s in "catalog","order","notification","payment","inventory","api-gateway") {
  docker build -t ghcr.io/goxzyp/sistemasdistribuidos/${s}:dev ./$s
}

kubectl apply --server-side -k k8s
kubectl get pods -n ticketmei -w
```

- API Gateway: http://localhost:8000
- Grafana: http://localhost:3000 (admin / admin)
- Prometheus: `kubectl port-forward -n ticketmei svc/prometheus 9090:9090`
- RabbitMQ UI: `kubectl port-forward -n ticketmei svc/rabbitmq 15672:15672`

`--server-side` is needed because the RabbitMQ dashboard ConfigMap is larger than
the 256 KB limit of the client-side apply annotation.

## 2. Argo CD

```powershell
kubectl create namespace argocd
kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl get pods -n argocd -w

# admin password and UI (https://localhost:8080, user: admin)
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | % { [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_)) }
kubectl port-forward -n argocd svc/argocd-server 8080:443

# tell Argo CD to follow the k8s/ folder of this repository (only once)
kubectl apply -f argocd/application.yaml
```

Argo CD checks Git every ~3 minutes; use **Refresh** in the UI to speed it up.

## 3. Rate limiter demo (POST /orders: 5 requests/s per IP)

```powershell
docker run --rm -i -v "${PWD}/k6:/scripts" -e BASE_URL=http://host.docker.internal:8000 grafana/k6:0.49.0 run /scripts/rate-limit-test.js
```

## 4. Saga test (1000 purchases of 20 tickets)

The test calls the services directly (the gateway rate limiter would answer
429 to almost everything), so open one terminal per port-forward:

```powershell
kubectl port-forward -n ticketmei svc/order 8002:8002
kubectl port-forward -n ticketmei svc/inventory 8005:8005
kubectl port-forward -n ticketmei svc/payment 8004:8004
```

```powershell
# every ticket is sold, 980 purchases are rejected for lack of stock
docker run --rm -i -v "${PWD}/k6:/scripts" -e ORDER_URL=http://host.docker.internal:8002 -e INVENTORY_URL=http://host.docker.internal:8005 -e PAYMENT_URL=http://host.docker.internal:8004 grafana/k6:0.49.0 run /scripts/saga-test.js

# same test with the Payment service failing, so the Saga compensations show up
docker run --rm -i -v "${PWD}/k6:/scripts" -e MODE=faults -e ORDER_URL=http://host.docker.internal:8002 -e INVENTORY_URL=http://host.docker.internal:8005 -e PAYMENT_URL=http://host.docker.internal:8004 grafana/k6:0.49.0 run /scripts/saga-test.js
```

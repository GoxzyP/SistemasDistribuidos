# Sistemas Distribuídos

## Project overview

**TicketMEI** is a  microservices-based ticket purchasing and event management platform designed to handle high-concurrency traffic efficiently. The system solves the challenge of traffic spikes during popular event ticket drops by decoupling critical ordering paths from non-critical background tasks.

An **API Gateway** acts as the single public entry point, protecting backend services using a Redis-backed rate limiter. 
The **Catalog Service** manages event inventory and pricing, while the **Order Service** processes purchases and validates requests. 
To prevent latency degradation under heavy load, order confirmations are pushed asynchronously via **RabbitMQ** to a decoupled **Notification Service**, ensuring high throughput, fault tolerance, and resilience (validated using `k6` performance testing).

---



## Central Dashboard & Access Hub

Quick reference for all local services, dashboards, and default credentials included in the Docker Compose stack:

| Service / Tool | Local URL / Endpoint | Default Credentials / Access Notes |
| :--- | :--- | :--- |
| **API Gateway** *(Public Entry)* | `http://localhost:8000` | Public entry point; rate-limited on `/orders` (max 5 req/sec/IP) |
| **Grafana Dashboard** | `http://localhost:3000` | **Username:** `admin` / **Password:** `admin` |
| **Prometheus UI** | `http://localhost:9090` | Direct metric scraper & query explorer |
| **RabbitMQ Management UI** | `http://localhost:15672` | **Username:** `guest` / **Password:** `guest` |
| **Catalog Service** *(Internal)* | `http://localhost:8001` | Manages events and pricing |
| **Order Service** *(Internal)* | `http://localhost:8002` | Processes orders and publishes events |
| **Notification Service** *(Async)* | Logged to `./notification/logs` | Consumes messages from RabbitMQ queue |

---

## Services and endpoints

| Service | Method | Endpoint | Purpose | Inputs and response |
|---|---|---|---|---|
| **API Gateway** | GET | `/events` | Retrieve all available events | Response: `200 OK` (Array of events) |
| **API Gateway** | GET | `/events/:id` | Retrieve details for a specific event | Path: `id`<br>Response: `200 OK`, `404 Not Found` |
| **API Gateway** | POST | `/orders` | Submit a new ticket order (Rate-limited) | Body: `customer_id`, `customer_email`, `event_id`, `quantity`<br>Response: `202 Accepted`, `400`, `429`, `502`, `503` |
| **API Gateway** | GET | `/metrics` | Expose Prometheus metrics for monitoring | Response: `200 OK` (Prometheus text format) |
| **Order Service** | POST | `/orders` | Internal order creation & queue publishing | Body: `customer_id`, `customer_email`, `event_id`, `quantity`<br>Response: `202 Accepted`, `503` |
| **Order Service** | GET | `/orders/:id` | Retrieve stored order state | Path: `id`<br>Response: `200 OK`, `404 Not Found` |
| **Notification Service** | *Async* | RabbitMQ Queue (`order_created`) | Consumes order creation events and logs delivery | Payload: `type`, `order_id`, `email` |

## Add-ons and dependencies

| Add-on/dependency | Version | Purpose | Setup/configuration |
|---|---|---|---|
| **Node.js / Express** | Latest LTS | Core runtime and web framework for microservices | Local build via Dockerfiles |
| **Redis** | `7-alpine` | Fast atomic storage for API Gateway rate limiting | Docker container (`redis`) |
| **RabbitMQ** | `3.13-management` | Asynchronous message broker for event-driven decoupling | Docker container (`rabbitmq`) |
| **Prometheus** | `v2.50.0` | Metrics collection and storage engine | Configured via `./prometheus/prometheus.yml` |
| **Grafana** | `10.3.1` | Visualization dashboards for system metrics | Configured via `./grafana/provisioning` |
| **prom-client** | Latest | Prometheus metrics instrumentation for Node.js | `npm install prom-client` |
| **amqplib** | Latest | AMQP 0-9-1 client library for Node.js / RabbitMQ | `npm install amqplib` |
| **k6** | `0.49.0` | Performance and load testing tool | Executed via Docker profile |

---

## Setup and running

1. **Start all infrastructure and microservices via Docker Compose:**
   ```bash
   docker compose up --build -d


## Testing and usage

1. **Example to make and order**

curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust-123",
    "customer_email": "user@example.com",
    "event_id": "evt-101",
    "quantity": 2
  }'

2. **Example to fetch all events**
curl -X GET http://localhost:8000/events

3. **Testing load via script load-test.js**
docker compose --profile testing run --rm k6

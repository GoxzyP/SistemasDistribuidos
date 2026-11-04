# Sistemas Distribuídos

## Project overview

**TicketMEI** is a  microservices-based ticket purchasing and event management platform designed to handle high-concurrency traffic efficiently. The system solves the challenge of traffic spikes during popular event ticket drops by decoupling critical ordering paths from non-critical background tasks.

An **API Gateway** acts as the single public entry point, protecting backend services using a Redis-backed rate limiter. 
The **Catalog Service** manages event inventory and pricing, while the **Order Service** processes purchases and validates requests. 
To prevent latency degradation under heavy load, order confirmations are pushed asynchronously via **RabbitMQ** to a decoupled **Notification Service**, ensuring high throughput, fault tolerance, and resilience (validated using `k6` performance testing).

---

## Services and endpoints

Document each service's endpoints below. Include the HTTP method, path, purpose, required inputs, and response. Replace the placeholders with the actual API routes.

| Service | Method | Endpoint | Purpose | Inputs and response |
|---|---|---|---|---|
| [Service name] | [GET/POST/PUT/DELETE] | `[/path]` | [Purpose] | [Parameters/body and response] |

### Service details

#### [Service name]

- **Responsibility:** [What this service does]
- **Base URL/port:** [Address or service discovery details]
- **Dependencies:** [Services or external APIs it calls]
- **API documentation:** [Link, if available]

## Add-ons and dependencies

List all frameworks, libraries, infrastructure components, and tools used by the project, with versions and their purpose.

| Add-on/dependency | Version | Purpose | Setup/configuration |
|---|---|---|---|
| [Name] | [Version] | [How it is used] | [Install or configuration notes] |

## Setup and running

1. Install the required runtimes and dependencies: [instructions].
2. Configure required environment variables or configuration files: [instructions].
3. Start the services: `[commands]`.
4. Check that the application is running at [URL or health endpoint].

## Configuration

Document required settings here. Do not include passwords, API keys, or other secrets.

| Setting | Required? | Description/example |
|---|---|---|
| [Variable or setting] | [Yes/No] | [Purpose and safe example] |

## Example usage

```http
[METHOD] [BASE_URL]/[endpoint]
Content-Type: application/json

{ "example": "Replace with a request supported by the project" }
```

**Expected response:** [Document the actual status code and response format.]

> Replace bracketed placeholders with verified project details. The supplied file contained no implementation, endpoint, or dependency information, so specific services and add-ons cannot be named accurately yet.

# Kafka Producer & Consumer Tuning Workshop

A hands-on workshop for learning how to **tune Kafka producers and consumers** for
throughput, latency, and reliability. You'll run a real Kafka cluster and two sample
apps — one in **Spring Boot (Java)** and one in **NestJS (TypeScript)** — then change
producer/consumer settings and watch the effect on performance and delivery guarantees.

> New to Kafka? Don't worry — each concept is explained as we go. The goal is to build
> intuition for *which knob to turn and why*, not to memorize every config.

---

## What you'll learn

- How a Kafka producer turns your messages into batched, compressed network requests —
  and how `acks`, `batch.size`, `linger.ms`, and `compression.type` trade latency for
  throughput and durability.
- How a Kafka consumer fetches, processes, and commits offsets — and how `fetch.min.bytes`,
  `max.poll.records`, and commit strategy affect throughput and at-least-once vs.
  at-most-once delivery.
- How **consumer groups** and partitions control parallelism and ordering.
- How to measure the impact of a change instead of guessing.

---

## Architecture

```
┌─────────────────┐        ┌─────────────────┐
│  Spring Boot     │        │  NestJS          │
│  producer/       │        │  producer/       │
│  consumer (Java) │        │  consumer (TS)   │
└────────┬─────────┘        └────────┬─────────┘
         │                           │
         └───────────┬───────────────┘
                     ▼
         ┌───────────────────────────┐
         │  Kafka cluster (Strimzi)   │
         │  on a Hetzner server       │
         │  via Docker / k3s          │
         └───────────────────────────┘
```

- **Kafka cluster** — managed by [Strimzi](https://strimzi.io/), running on a Hetzner
  cloud server. Strimzi is a Kubernetes operator, so the simplest path is a lightweight
  k3s cluster on the Hetzner box; you can also run Kafka in plain Docker for local dev.
  The Hetzner side (server, firewall, SSH key, k3s bootstrap) is provisioned **entirely
  with Terraform** — see [Setup](#setup).
- **Spring Boot app** — Java producer + consumer using Spring for Apache Kafka.
- **NestJS app** — TypeScript producer + consumer (e.g. via `kafkajs` / `@nestjs/microservices`).

The two apps do the same job in different stacks so you can compare how tuning concepts
map across ecosystems.

---

## Project structure

```
kafka/
├── readme.md                  # this file
├── infra/
│   └── terraform/             # Hetzner server + firewall + k3s (the ONLY way we touch Hetzner)
│       ├── main.tf            # hcloud provider, server, firewall, k3s via cloud-init
│       ├── variables.tf
│       ├── outputs.tf         # exposes kubeconfig for kubectl
│       └── terraform.tfvars.example
├── kafka/
│   └── kafka-cluster.yaml     # Strimzi Kafka CR (KRaft) + KafkaTopic workshop.events
├── spring-boot/               # Java producer/consumer (Spring for Apache Kafka)
│   ├── src/main/java/com/example/kafkaworkshop/
│   ├── src/main/resources/application.yml
│   ├── pom.xml + mvnw
│   └── README.md
├── nestjs/                    # TypeScript producer/consumer (kafkajs)
│   ├── src/                   # producer + consumer services, /produce controller
│   ├── package.json
│   └── README.md
├── benchmarks/                # producer-perf.sh / consumer-perf.sh + README
└── exercises/                 # step-by-step hands-on walkthroughs (start at 01)
```

Each app and the benchmarks have their own README with run instructions and the full
list of env-var tuning knobs.

---

## Prerequisites

- [Terraform](https://www.terraform.io/) and a Hetzner Cloud API token — **all Hetzner
  resources are managed through Terraform**, never the Hetzner console or `hcloud` CLI
- Docker (and `kubectl` to talk to the k3s cluster Terraform creates)
- Java 17+ and Maven/Gradle (for the Spring Boot app)
- Node.js 18+ and a package manager (for the NestJS app)
- Basic command-line comfort; no prior Kafka experience required

---

## Setup

> The commands below are **examples** — adjust hostnames, versions, and paths to your setup.

### 1. Provision the Hetzner server with Terraform

All Hetzner infrastructure (server, firewall, SSH key) lives in Terraform and is
bootstrapped with k3s via cloud-init. **Don't create or change Hetzner resources by hand**
— if something needs to change, change the Terraform and re-apply.

```bash
cd infra/terraform
export HCLOUD_TOKEN=...          # your Hetzner Cloud API token

terraform init
terraform plan                   # review what will be created
terraform apply                  # provisions the server + k3s

# Pull the kubeconfig Terraform exposes, then point kubectl at the new cluster
terraform output -raw kubeconfig > ~/.kube/kafka-workshop.yaml
export KUBECONFIG=~/.kube/kafka-workshop.yaml
```

### 2. Install Strimzi and deploy Kafka

Once k3s is up, the Kafka side is plain Kubernetes — no further Hetzner interaction.

```bash
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Deploy the workshop Kafka cluster + topic (KRaft, single node)
kubectl apply -f kafka/kafka-cluster.yaml -n kafka
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

> Prefer plain Docker for local experiments? Run a single-broker Kafka with Docker
> Compose instead — fine for learning, not for the tuning load tests. (No Hetzner or
> Terraform needed for the local path.)

### 3. Run the Spring Boot app

```bash
cd spring-boot
# point at your broker (default localhost:9092); see spring-boot/README.md for all knobs
KAFKA_BOOTSTRAP_SERVERS=<broker:port> ./mvnw spring-boot:run
# then: curl -X POST "http://localhost:8080/produce?count=1000&size=200"
```

### 4. Run the NestJS app

```bash
cd nestjs
npm install
KAFKA_BOOTSTRAP_SERVERS=<broker:port> npm run start:dev   # see nestjs/README.md for all knobs
# then: curl -X POST "http://localhost:3000/produce?count=1000&size=200"
```

---

## Workshop modules

> Hands-on walkthroughs live in [`exercises/`](exercises/) — start with
> [Exercise 1: Producer performance tuning](exercises/01-producer-performance-tuning.md).

1. **Hello, Kafka** — produce and consume a single message end to end; understand topics,
   partitions, and offsets.
2. **Producer tuning** — measure throughput, then tune batching, compression, and `acks`;
   observe the latency/throughput/durability trade-offs.
   → [exercises/01-producer-performance-tuning.md](exercises/01-producer-performance-tuning.md)
3. **Consumer tuning** — tune fetch sizes and poll batches; explore consumer groups and
   rebalancing; compare commit strategies.
4. **Delivery guarantees** — at-most-once vs. at-least-once vs. exactly-once; idempotent
   producers and transactions.
5. **Compare stacks** — repeat key experiments in both Spring Boot and NestJS.

---

## Kafka tuning concepts (reference)

A quick reference for the knobs you'll touch. Defaults vary by client/version — always
check your client's docs.

### Producer

| Setting | What it does | Tuning intuition |
| --- | --- | --- |
| `acks` | How many replicas must acknowledge a write (`0`, `1`, `all`) | `all` = strongest durability, higher latency. `0` = fire-and-forget, can lose data. |
| `batch.size` | Max bytes per partition batch | Bigger batches → higher throughput, slightly higher latency. |
| `linger.ms` | How long to wait to fill a batch before sending | A few ms of linger lets batches fill, boosting throughput under load. |
| `compression.type` | `none`/`gzip`/`snappy`/`lz4`/`zstd` | Trades CPU for network/disk. `lz4`/`zstd` are good defaults for throughput. |
| `enable.idempotence` | Dedupes retries so a record is written once | Enable for safe retries without duplicates. |
| `max.in.flight.requests.per.connection` | Unacked requests per connection | Keep ≤5 with idempotence to preserve ordering. |
| `retries` / `delivery.timeout.ms` | Retry behavior on transient failures | Higher retries improve reliability; bound the total with the delivery timeout. |

### Consumer

| Setting | What it does | Tuning intuition |
| --- | --- | --- |
| `fetch.min.bytes` | Min data the broker waits to accumulate before responding | Higher → fewer, fatter fetches (throughput); adds a little latency. |
| `fetch.max.wait.ms` | Max wait to satisfy `fetch.min.bytes` | Caps the latency cost of the setting above. |
| `max.poll.records` | Max records returned per `poll()` | Lower if per-record processing is slow, to avoid poll timeouts. |
| `max.poll.interval.ms` | Max time between polls before the consumer is considered dead | Raise if processing a batch legitimately takes a while. |
| `enable.auto.commit` | Auto-commit offsets on a timer | Off + manual commit gives precise at-least-once control. |
| `auto.offset.reset` | Where to start with no committed offset (`earliest`/`latest`) | `earliest` to replay from the start; `latest` for only-new messages. |
| Consumer group + partitions | Unit of parallelism and ordering | Max parallelism = number of partitions; ordering is guaranteed only within a partition. |

### Rules of thumb

- **Throughput vs. latency**: batching and larger fetches raise throughput at the cost of
  a little latency. Tune toward whichever your use case actually needs.
- **Durability costs latency**: `acks=all` and replication protect data but slow each write.
- **Parallelism is bounded by partitions**: you can't have more actively-consuming members
  in a group than there are partitions.
- **Measure, don't guess**: change one setting at a time and compare before/after numbers.

---

## Scaffold status

The full tree above is scaffolded and ready to flesh out:

- [x] Terraform for Hetzner (`infra/terraform/`) — provider, server, firewall, k3s
- [x] Strimzi cluster + topic manifest (`kafka/kafka-cluster.yaml`)
- [x] Spring Boot producer/consumer with env-var tuning knobs (`spring-boot/`)
- [x] NestJS producer/consumer with env-var tuning knobs (`nestjs/`)
- [x] Perf benchmark scripts (`benchmarks/`)

Still to do for your environment:

- [ ] Copy `infra/terraform/terraform.tfvars.example` → `terraform.tfvars`, set your token and **lock `allowed_cidrs` to your IP**
- [ ] `terraform apply`, install Strimzi, apply the cluster manifest
- [ ] Run an app, fire `/produce`, and watch consumer throughput
- [ ] Capture expected results/screenshots for each tuning experiment

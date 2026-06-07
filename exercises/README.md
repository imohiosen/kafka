# Workshop exercises

Step-by-step, hands-on walkthroughs for the workshop. Each one assumes the cluster from the
root [readme](../readme.md) is up and `kubectl` works, and each ends by reminding you how to
tear things down so you don't keep paying for the Hetzner server.

Work through them in order, or jump to the topic you care about. Change **one** knob at a
time, measure, and compare — that habit is the whole point.

| # | Exercise | What you'll tune |
| --- | --- | --- |
| 01 | [Producer performance tuning](01-producer-performance-tuning.md) | `linger.ms`, `batch.size` — throughput vs. latency |
| 02 | _Consumer tuning_ (planned) | `fetch.min.bytes`, `max.poll.records`, consumer groups |
| 03 | _Delivery guarantees_ (planned) | `acks`, idempotence, transactions |
| 04 | _Compare stacks_ (planned) | repeat key experiments in Spring Boot vs. NestJS |

These map to the **Workshop modules** in the root [readme](../readme.md#workshop-modules).

## Conventions used across exercises

- Commands assume you've `cd`'d into the directory named at the top of each step.
- The benchmark scripts run *inside the broker pod* via `kubectl exec`, so they use the
  cluster-internal bootstrap (`my-cluster-kafka-bootstrap:9092`) — no external IP needed.
  If your broker pod isn't `my-cluster-dual-role-0`, prefix commands with `KAFKA_POD=<name>`.
- The sample apps read every tuning knob from environment variables (see each app's README),
  so you change behavior **without editing code** — set the var, restart, re-run `/produce`.

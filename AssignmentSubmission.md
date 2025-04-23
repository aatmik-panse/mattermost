# Assignment Submission

## 1. Approach

To enable High Availability (HA) and horizontal scalability in the open‑source Mattermost Team Edition without an Enterprise license, I:

1. **Analyzed existing limitations**: Confirmed that Team Edition lacks built‑in clustering (no event propagation across nodes).
2. **Adopted a community pattern**: Integrated a Redis Pub/Sub layer to fan out WebSocket events between server nodes.
3. **Patched the official repo**: Made minimal, focused edits directly in `hub.go` and startup code rather than maintaining a separate fork.
4. **Containerized the solution**: Wrote a multi‑stage Dockerfile and a `docker‑compose.yml` to spin up multiple Mattermost nodes, Redis, Postgres, and a load‑balancer.
5. **Tested locally**: Verified real‑time messaging, presence, and typing sync across nodes via both direct ports and an Nginx proxy.

## 2. System Architecture

```plaintext
                ┌──────────┐          ┌────────────┐
         ┌─────▶│ Mattermost│◀─── PubSub ──▶│ Mattermost│◀────┐
         │      │  Node A   │             │  Node B   │     │
         │      └──────────┘             └────────────┘     │
         │                                              │
 Client──┼─────────────┐   ┌────────────┐   ┌────────────┐  │
 Browser │  WebSocket  │   │  Redis     │   │ PostgreSQL │  │
         │  Upgrade    │   │  Pub/Sub   │   │  Master    │  │
         └─────────────┘   └────────────┘   └────────────┘  │
                                                        │
                       ┌────────────┐                   │
                       │  Nginx     │◀──────────────────┘
                       │ Load‑Balancer │
                       └────────────┘
```

-   **Mattermost Nodes (A, B, …)**: Each runs patched `hub.go` that both publishes outbound WebSocket events to Redis and subscribes to peer events.
-   **Redis Pub/Sub**: Transports ephemeral events (new messages, presence, typing) between nodes.
-   **PostgreSQL**: Single source of truth for persistent data (posts, channels, users).
-   **Nginx Load‑Balancer**: Distributes client HTTP/WebSocket connections across nodes; handles upgrade headers.

## 3. Key Code Changes

1. **Imports & Hub struct** (`server/v8/channels/web/hub.go`):
    - Added `github.com/redis/go-redis/v9`, `context`, `encoding/json`, `fmt`.
    - Introduced `redisClient *redis.Client` field in `Hub`.
2. **Startup wiring** (`platform/hubStart`):
    - Initialized `redis.Client` when `RedisSettings.Enable=true`.
    - Attached `redisClient` to each Hub instance and launched `clusterSubscribe` goroutine.
3. **Broadcast interception** (`hub.go` broadcast loop):
    - Before local fan‑out, JSON‑marshal `WebSocketEvent` and `Publish` to `mattermost_ws_<ChannelID>` channel.
4. **Cluster subscription** (new `clusterSubscribe`):
    - PSubscribed to `mattermost_ws_*`, unmarshaled events, reinjected via `h.localBroadcast`.
5. **Docker & Compose**:
    - Multi‑stage `Dockerfile` to build patched binary.
    - `docker‑compose.yml` mapping host ports (`8065`,`8066`,`8067`) to container ports.
    - Nginx config for WebSocket proxy.

## 4. Challenges Faced & Resolutions

| Challenge                                         | Resolution                                                                                                  |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **No native clustering in Team Edition**          | Researched community forks; adopted Redis Pub/Sub pattern to emulate enterprise HA.                         |
| **Merging community code vs. upstream drift**     | Chose minimal patches (3 splice points) in official repo to ease future merges.                             |
| **Port-binding confusion** (`expose` vs. `ports`) | Added explicit `ports:` mappings for followers (8066, 8067) so they bind on host.                           |
| **WebSocket proxying**                            | Configured Nginx to handle `Upgrade`/`Connection` headers for sticky‑less load balancing.                   |
| **Ensuring event ordering & consistency**         | Kept a single DB for persistence; Redis only used for ephemeral event fan‑out (DB remains source of truth). |
| **Local testing of multi‑node setup**             | Used Docker Compose, healthchecks, and direct port access to validate real‑time sync across browser tabs.   |

---

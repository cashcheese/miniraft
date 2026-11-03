# MiniRaft — Distributed Drawing Board

A real-time collaborative drawing board backed by a from-scratch implementation of the **Raft consensus algorithm**. Every stroke drawn in the browser is replicated across a 3-node cluster and only shown to clients once a majority of replicas have committed it, so the board stays consistent even if a node (including the leader) goes down.

## Architecture

```
  Browser clients  ──WebSocket──►  Gateway (:3000)
                                      │  forwards strokes to the current leader
                                      ▼
              ┌────────────┬────────────┬────────────┐
              │ replica1   │ replica2   │ replica3   │
              │ :4001      │ :4002      │ :4003      │
              └────────────┴────────────┴────────────┘
                 leader election · log replication · heartbeats
```

| Service      | Port | Role |
|--------------|------|------|
| `gateway`    | 3000 | Serves the frontend, holds WebSocket connections, discovers the leader, forwards strokes and broadcasts committed ones |
| `replica1-3` | 4001–4003 | Raft nodes: elect a leader, replicate the stroke log, commit on majority |
| `frontend`   | —    | Single-page canvas client (served by the gateway) |

### How a stroke flows

1. The browser sends the stroke to the gateway over WebSocket.
2. The gateway forwards it to the current leader (re-discovering the leader if the request fails).
3. The leader appends it to its log and sends `AppendEntries` to its peers.
4. Once a majority (2 of 3) acknowledge, the entry is committed and the leader notifies the gateway.
5. The gateway broadcasts the committed stroke to every connected client. New clients receive the full committed history on connect.

### Raft details

- **Election timeout:** randomized 500–800 ms
- **Heartbeat interval:** 150 ms
- **Quorum:** 2 of 3 nodes
- Nodes step down on seeing a higher term; followers reject entries that fail the `prevLogIndex` / `prevLogTerm` consistency check and the leader backs off `nextIndex` to resync them.

## Project structure

```
.
├── docker-compose.yml   # Runs the gateway and the 3 replicas on a shared network
├── frontend/
│   └── index.html       # Canvas UI (color, brush size, clear, connection/leader status)
├── gateway/
│   ├── Dockerfile
│   ├── package.json
│   └── server.js        # Express + ws gateway
├── replica1/            # Raft node (each replica is its own service so it can be
├── replica2/            #   edited/restarted independently to test failover)
└── replica3/
    ├── Dockerfile
    ├── package.json
    └── server.js        # Raft implementation
```

## Getting started

**Prerequisites:** Docker and Docker Compose, with ports 3000 and 4001–4003 free.

```bash
git clone https://github.com/cashcheese/miniraft.git
cd miniraft
docker compose up --build
```

Then open **http://localhost:3000** in a few browser tabs and start drawing; strokes show up in every tab.

Source directories are mounted into the containers and run under `nodemon`, so code changes trigger a restart automatically.

## API

### Gateway (`:3000`)

| Method | Path         | Description |
|--------|--------------|-------------|
| `POST` | `/stroke`    | Forward a stroke to the leader |
| `POST` | `/committed` | Called by the leader when an entry commits; broadcasts it to clients |
| `POST` | `/clear`     | Clear the board for all clients |
| `GET`  | `/status`    | Current leader URL and connected client count |

### Replica (`:4001`–`:4003`)

| Method | Path              | Description |
|--------|-------------------|-------------|
| `POST` | `/request-vote`   | Raft `RequestVote` RPC |
| `POST` | `/append-entries` | Raft `AppendEntries` RPC (log replication) |
| `POST` | `/heartbeat`      | Leader heartbeat |
| `POST` | `/stroke`         | Append a stroke (leader only) |
| `GET`  | `/sync-log?from=N`| Fetch log entries from index `N` |
| `GET`  | `/status`         | Node state, term, leader, log length and commit index |

## Testing failover

```bash
# See which node is leader
curl -s localhost:4001/status; curl -s localhost:4002/status; curl -s localhost:4003/status

# Kill the leader (e.g. replica2)
docker compose stop replica2

# Watch the remaining nodes elect a new leader
docker compose logs -f replica1 replica3

# Bring it back; it rejoins as a follower and catches up
docker compose start replica2
```

Drawing continues to work while any 2 of the 3 replicas are up.

## Tech stack

Node.js 20 · Express · ws · Axios · Docker Compose · HTML5 Canvas

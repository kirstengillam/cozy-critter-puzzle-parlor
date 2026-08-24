# Implementation Plan

This supplements [cozy_critter_context.md](cozy_critter_context.md) (the PRD) with the finer-grained,
decided steps we're actually building against. Update this file as decisions change.

## Status

- **Milestone 1 (Local Loopback Foundation): done (1.1–1.7).** Came back to the skipped 1.6/1.7 after
  Milestone 3 started; both done now.
- **Milestone 2 (Room Sync): done (2.1–2.18).** Private code-joined rooms, movement, stub-filtered chat,
  and Kubernetes parity (Strimzi on `kind`, containerized gateway) all verified.
- **Milestone 3 (Word Game & Economy): done (3.1–3.10).** Word list, session lifecycle, guess
  evaluation, ephemeral economy with a real HMAC-verified ledger, and the frontend UI all built and
  verified — including a full manual playthrough to a win with the correct reward credited.
- **Between Milestone 3 and 4 — UI polish pass (not a numbered step):** checkerboard placeholder floor
  texture, per-player name labels on avatars, and an in-world game table (a clickable map tile) as a
  second way to start the word game, alongside the existing lobby button. See "Locked-in decisions."
- **Milestone 4:** not started.

## Locked-in decisions

- **Solo project, portfolio-oriented.** Kafka + Kubernetes are used deliberately to demonstrate
  those skills, not because a few-dozen-concurrent-user cozy game strictly requires them at this scale.
- **Backend:** Go, for the WebSocket gateway and consumer workers.
- **Frontend:** Phaser.js.
- **Local dev loop:** Docker Compose for fast inner-loop iteration (single KRaft Kafka broker, no
  Zookeeper). Kubernetes (via `kind`) is the parity/deployment layer, built once services are stable
  enough to containerize — not the day-one edit/run loop.
- **Kafka on Kubernetes:** Strimzi operator (not hand-rolled StatefulSets) — matches real-world
  practice and is a stronger resume/interview story.
- **Gateway topology:** single instance for now. Multi-instance horizontal scaling (consumer-group-per-
  instance fan-out) is explicitly deferred to a later, separately-scoped milestone.
- **Rooms are private, code-joined — not public matchmaking.** A room is created with a generated join
  code; players only enter a room they have the code for. This is the moderation mitigation for now:
  since room membership is already restricted to known participants, chat risk is much lower than open
  public rooms. Full moderation (rate limiting, reporting, bans) is deferred; a stub filter still runs
  so the pipeline shape is proven early.
- **Chat filter for this milestone:** a stub wordlist check proving the
  `PENDING_VALIDATION → filtered → broadcast` pipeline, not real moderation.
- **Git:** initialized, PRD/docs committed as the first commit.
- **Distribution is not a committed goal.** Public discovery (itch.io/CrazyGames/cozy communities) is a
  possibility, not a design assumption — this may end up self-hosted for a known group instead. See the
  PRD's "Distribution note."
- **First mini-game: a Wordle-style word game**, not Sudoku, despite the PRD roadmap's wording — chosen
  over Sudoku/typing-test for Milestone 3.
- **Player identity is ephemeral for now**, not persistent accounts. Currency and inventory live in
  memory, scoped to the session, and reset on disconnect. This proves the economy/inventory pipelines
  without building auth yet.
- **Postgres is the decided future persistence store** for accounts/inventory/ledger, once persistence
  is actually built — not built now, but decided so it isn't re-litigated later. The event *schemas* for
  currency and inventory should already look like the real (Postgres-backed, double-entry) thing even
  while the storage backend is an in-memory map — only the storage layer is the deferred part.
- **Kafka writers and readers retry transient errors** (`produce` / `startPartitionConsumer` in
  `internal/gateway/gateway.go`, currently 25 attempts × 300ms). Right after `EnsureTopic` creates a
  topic, a connection's cached metadata can be stale for a moment even on a single broker, causing an
  immediate produce or partition-leader lookup to fail — worse when several topics are created
  back-to-back against a freshly started broker (hit this for real in Milestone 3 testing; the original
  10×200ms budget wasn't always enough).
- **Word list source: a public GitHub mirror of the original NYT Wordle client's word lists**, not the
  originally-linked Kaggle dataset (which requires a login to download). Same underlying data, widely
  re-hosted across the open-source Wordle-clone ecosystem. 2315 answers / 12972 total allowed guesses,
  embedded via `go:embed` in `internal/wordgame/data/`.
- **No sprite/background art style decided yet.** Use simple placeholder shapes (colored circles/
  rectangles, maybe a label) for critters and rooms in the meantime. Load sprites from a small config/
  manifest (critter type → texture key) rather than hardcoding shapes inline in scene code, so that
  real art (commissioned from a friend, or a self-made early pass) is a data/asset swap later, not a
  rendering-logic rewrite. Relevant starting at Milestone 2 step 2.7. The room floor is now a checkerboard
  placeholder (`drawFloor` in `frontend/main.js`) rather than a flat rectangle — same "placeholder, still
  swappable" spirit, just less bare.
- **Word game has two entry points: the lobby button and an in-world game table.** A fixed map tile
  (`GAME_TABLE_CELL` in `frontend/main.js`, currently (12, 1)) that a player walks onto (a normal
  click-to-move) to trigger `START_GAME` on arrival — matches the PRD's "game tables" framing. Kept
  alongside the standalone lobby button rather than replacing it, for ease of testing; may remove the
  button later. The word game still doesn't require room membership either way (per the earlier
  decision) — the table is just a second, in-world trigger for the same room-independent game.

## Open / deferred (not blocking the next milestones)

- Live cloud deployment: undecided. Local + kind + strong docs is enough for now; revisit once the
  core loop works.
- Full chat moderation system (rate limiting, reporting, mutes/bans).
- Multi-instance gateway scaling design. Note: a consumer-group-based broadcaster (auto-splitting
  partitions/rooms across instances) turned out to have a startup race — see 2.6 — so this needs an
  actual design, not just "add a GroupID," when it's picked up.
- Persistent accounts (Postgres-backed): schema is decided (see above) but not built — inventory/
  currency are ephemeral until this lands.

---

## Milestone 1: Local Loopback Foundation

Goal: a single Docker Compose stack where a browser tab round-trips a message through Kafka and back.

1.1 ✅ Scaffold the repo: `cmd/gateway` (Go entrypoint), `internal/` (packages), `frontend/` (static
    Phaser.js client), `deploy/compose/`, `deploy/k8s/` (empty for now), `go.mod`.
1.2 ✅ `docker-compose.yml`: single KRaft-mode Kafka broker (no Zookeeper) with a healthcheck.
1.3 ✅ Minimal Go WebSocket gateway: accept a connection, on any inbound message produce it to an
    `echo-test` topic, consume that same topic, push whatever comes back out over the socket.
    (`internal/gateway`; had to fix a consumer-offset race — see commit 1e8904e.)
1.4 ✅ Minimal static `index.html` + Phaser.js stub: connect to the gateway over WebSocket, send a test
    message on a button click, render whatever comes back. (Phaser vendored locally, not CDN; needed an
    origin allowlist since frontend/gateway run on different dev ports — see commit 94125de.)
1.5 ✅ Manual verification: open one tab, send a message, confirm it round-trips through Kafka and back
    to the same tab within reasonable latency. (Verified in a real browser pane, not just the automated
    test — folded into the 1.4 session rather than a separate pass.)
1.6 ✅ One-command bootstrap (`Makefile`): `make up`/`down` wrap the Compose stack (waits for the Kafka
    healthcheck); `gateway`/`frontend` run the two dev-loop pieces; `vet`/`build`/`test`/`docker-build`
    round it out. Picked up after Milestone 3 started, not immediately after 1.5.
1.7 ✅ GitHub Actions CI (`.github/workflows/ci.yml`): `go vet` + `go build` + `go test` on push/PR, with
    a Kafka service container (same single-broker KRaft config as Compose) so the Kafka-backed tests
    actually run in CI rather than skipping. Verified the exact service-container config locally via a
    bare `docker run` before committing, and checked the workflow with `actionlint` — but couldn't
    validate an actual Actions run, since this repo has no GitHub remote yet.

## Milestone 2: Room Sync — Presence, Movement, and Private-Room Chat

Goal: two browser tabs, each holding a join code for the same room, see each other's avatars move and
can chat, with messages passing through a (stub) filter.

**Rooms & joining**
2.1 ✅ Room lifecycle: a `CREATE_ROOM` message generates a short room code (e.g. 6 alphanumeric chars) and
    registers an empty room in the gateway's in-memory registry. In-memory is fine for now — rooms are
    ephemeral; persistence is a later concern. (`internal/room`, `internal/schema` — commit 21a5389.)
2.2 ✅ `JOIN_ROOM` message: client sends `{player_id, room_code}`; gateway validates the code exists,
    registers the connection under that room, rejects unknown codes. (Connection-to-room association is
    currently just a local variable in the handler; a real membership structure arrives with 2.5/2.6
    broadcast.)

**Movement**
2.3 ✅ Define the `player-positions` payload as Go structs matching the PRD schema; put them in a shared
    `internal/schema` package so gateway and any future consumers agree on the shape.
2.4 ✅ Create the `player-positions` Kafka topic, partitioned by `room_id` (6 partitions, `kafka.Hash`
    keyed by room code).
2.5 ✅ On a `MOVE` message from a joined client: validate shape, stamp `event_id`/`timestamp` server-side,
    produce to `player-positions` keyed by `room_id`.
2.6 ✅ Broadcast path: `internal/gateway/hub.go` tracks per-room connections/positions; a background
    reader consumes `player-positions` and fans out `PLAYER_MOVED` in-process. Originally used a
    consumer-group reader, matching the "seam for later multi-instance fan-out" idea below — but its
    join/rebalance handshake created a real race (a message produced right after startup could be
    missed entirely, since a fresh partition assignment only sees messages after it positions). Fixed by
    switching to direct per-partition readers positioned synchronously before startup returns (same
    technique as the Milestone 1 fix). Multi-instance fan-out therefore still needs a mechanism
    (deferred — see "Open / deferred" below), just not a consumer group as originally assumed.
2.7 ✅ Frontend: Phaser scene renders a simple grid room; other players are sprites; on a broadcast MOVE
    event, tween the corresponding sprite toward the target position. (`frontend/main.js`,
    `frontend/critters.js` for the swappable placeholder-shape manifest.) Hit a real bug: Phaser's
    `pointer.x/y` came back `Infinity` in testing, silently turning every click into a move to (0,0);
    fixed by reading the native DOM event's `offsetX/offsetY` instead.
2.8 ✅ Two-tab manual test: join both tabs to the same room code, move in Tab A, confirm Tab B updates.
    Verified via console logs that both tabs receive byte-identical `PLAYER_MOVED` payloads, and that
    single-tab rendering is correct end to end. A true side-by-side visual comparison wasn't reliable in
    this session's browser-automation tool specifically — it doesn't keep a second tab's viewport
    genuinely live once "selected," which starves Phaser's render loop for that tab — so this is
    confirmed at the data/single-tab level rather than a two-window screenshot. Worth a quick manual
    check with two real browser windows side by side if you want to see it live yourself.

**Chat (stub pipeline)**
2.9 ✅ Define `chat-messages` payload as Go structs (mirrors PRD schema), create the topic (6 partitions,
    same key-by-room-code pattern as `player-positions`).
2.10 ✅ On a `CHAT` message from a joined client: produce to `chat-messages` with `status:
     PENDING_VALIDATION`.
2.11 ✅ Stub filter consumer: consumes `chat-messages`, checks against a small hardcoded wordlist, marks
     `APPROVED` or `REJECTED`. `APPROVED` broadcasts to the room; `REJECTED` goes back only to the
     sender (new `hub.sendTo`), not the whole room. The direct-partition-reader setup from 2.6 was
     factored into a shared `startPartitionConsumer` since both movement and chat need it now.
2.12 ✅ Frontend: chat log + input alongside the room view; approved messages render normally, rejected
     ones render inline in a visibly distinct style rather than vanishing silently.
2.13 ✅ Two-tab manual test: chat from Tab A appears in Tab B; a wordlist-flagged message is visibly
     rejected. Verified via `TestChatApprovedBroadcastsAndRejectedStaysPrivate` (both parties see an
     approved broadcast, only the sender sees a rejection) plus a manual pass against the real binary.
     Also found and fixed a real produce-side race (see "Locked-in decisions" below).

**Kubernetes parity**
2.14 ✅ Containerize the gateway (`Dockerfile`): multi-stage, `golang:1.27-alpine` builder producing a
     static (`CGO_ENABLED=0`) binary, run as non-root on `alpine:3.20`.
2.15 ✅ Stand up a local `kind` cluster: `deploy/k8s/kind-config.yaml` maps NodePort 30080 to host
     `:8080`, so the existing frontend config (`ws://localhost:8080/ws`) works unchanged against either
     stack. `kind create cluster --name cozy-critter --config deploy/k8s/kind-config.yaml`.
2.16 ✅ Installed the Strimzi operator (bundle install, `kubectl create -f
     'https://strimzi.io/install/latest?namespace=kafka' -n kafka`) and applied
     `deploy/k8s/kafka.yaml` — a single dual-role (controller+broker) KRaft node, ephemeral storage,
     replication factor 1, mirroring the Compose setup's "not a production topology" scope. Strimzi
     1.2.0 dropped ZooKeeper support entirely (KRaft-only via `KafkaNodePool`), which simplified this
     versus what the plan originally anticipated.
2.17 ✅ `deploy/k8s/gateway.yaml`: Deployment (1 replica) + NodePort Service, pointed at
     `cozy-critter-kafka-bootstrap.kafka.svc.cluster.local:9092`. Built the image, `kind load
     docker-image cozy-critter-gateway:dev --name cozy-critter` (kind nodes can't see the host's Docker
     images otherwise), then `kubectl apply -f deploy/k8s/gateway.yaml`.
2.18 ✅ Re-ran the Milestone 2 checks (create/join, movement broadcast, chat approve + reject) against
     the `kind`-deployed stack via a throwaway two-client script hitting `localhost:8080` — same results
     as Compose. Cluster was torn down after (`kind delete cluster --name cozy-critter`) to free
     resources; recreate with the commands above whenever k8s parity needs re-checking.

## Milestone 3: Cheat-Proof Word Game & Economy Loop

Goal: a player plays a Wordle-style game entirely validated server-side, and a win credits their
(ephemeral, in-memory) currency balance — proving the puzzle-to-economy pipeline end to end.

**Word game**
3.1 ✅ Curate a word list bundled with the backend (a fixed answer list + a broader allowed-guesses list,
    Wordle-style, so players can't be blocked by "not a real word" on legitimate guesses).
    - The Kaggle datasets originally linked here require a login to actually download, so instead
      sourced from a public GitHub mirror of the original NYT Wordle client's word lists (no auth
      needed) — same underlying data, ubiquitously re-hosted across the open-source Wordle-clone
      ecosystem. 2315 answers, 12972 total allowed guesses. Embedded via `go:embed` in
      `internal/wordgame/data/`.
3.2 ✅ Define `game-sessions` payload as Go structs (session_id, player_id, word length, guesses,
    status) in the shared schema package; create the `game-sessions` topic. Unlike
    player-positions/chat-messages, this topic is a pure audit log — no consumer reads it back for
    broadcast, since GAME_STARTED/GUESS_RESULT go directly to the requesting connection (single-player).
3.3 ✅ `START_GAME` message: gateway picks a random target word server-side (never sent to the client),
    creates a session, produces a session-started event to `game-sessions`, responds to the client with
    just `session_id` + guesses-remaining. Not room-scoped — the client passes `player_id` explicitly
    rather than relying on prior room membership.
3.4 ✅ `GUESS` message: gateway validates the guess is in the allowed-guesses list, computes per-letter
    feedback (correct/present/absent) against the server-held target word, appends to session state,
    produces a guess-evaluated event, and returns the feedback to the client (not room-broadcast — this
    is single-player, private to that session).
3.5 ✅ Win/loss detection: on a correct guess or exhausted attempts, mark the session complete and
    produce a session-completed event (a second, distinct Kafka event from the guess-evaluated one, so
    the future economy consumer can react specifically to completions).
    (`internal/wordgame`, `internal/gateway/sessions.go` — commit 8126e4c.) Along the way, widened the
    Kafka produce/consumer retry budget after hitting a real flake: a freshly created broker under
    several topics being created back-to-back occasionally needed more than the original 2-second retry
    window for partition-leader metadata to propagate.

note: other word games and puzzle games will be added later, but starting with wordle-style game first for
 proof of concept of other features

**Economy (ephemeral)**
3.6 ✅ Define `economy-ledger` payload as Go structs matching the PRD schema (transaction_id, player_id,
    action CREDIT/DEBIT, amount, verification data) — build the real-looking schema even though storage
    is in-memory for now (see "Locked-in decisions"). `verification_hash` is a real HMAC-SHA256 over the
    transaction fields (`internal/gateway/economy.go`), not a decorative string — the ledger consumer
    actually recomputes and checks it.
3.7 ✅ On session-completed (win), produce a `CREDIT` event sized by guesses used (e.g. fewer guesses =
    more currency) — `wordgame.RewardForWin`, 60 down to 10 across the 6 allowed guesses. Done inline in
    `handleGuess` rather than as a separate consumer reacting to the `SESSION_COMPLETED` event — we
    already know the outcome synchronously at that point, so a fourth Kafka hop wasn't worth it.
3.8 ✅ Economy consumer (`StartEconomyLedger`): consumes `economy-ledger`, verifies each entry's HMAC
    before applying anything, applies CREDIT/DEBIT against an in-memory `map[player_id]balance`
    (mutex-guarded), pushes a `CURRENCY_BALANCE_UPDATED` event back to the client. Needed a new
    player-id-keyed connection registry in the hub (`registerPlayer`/`sendToPlayer`) since the word game
    isn't room-scoped, so the existing room-based routing couldn't reach the player.
3.9 ✅ Frontend: word-game UI (letter grid, on-screen keyboard, color feedback), currency balance visible
    and ticking up on a win. "Play Word Game" works independent of room join; the lobby no longer hides
    on room join so both can be used together.
3.10 ✅ Manual test: play a full game to a win, confirm balance increases by the expected amount; play a
     losing game, confirm no credit. Done live in a real browser (crane → shone → tense → dense, won in
     4, balance became exactly 30 = reward for 4 guesses) plus automated integration tests for both the
     win-credits and loss-credits-nothing paths.

## Milestone 4: Cosmetic Customization (Ephemeral Inventory)

Goal: spend earned currency on cosmetic items that visibly equip on the player's avatar, still within
the ephemeral (in-memory, session-scoped) identity model.

4.1 Define a static item catalog (hat, shirt, background, trail effect — id, name, price) as a Go
    config/struct; no DB table yet, this is fixed data for now.
4.2 In-memory per-player inventory (`map[player_id][]item_id`), alongside the Milestone 3 balance map.
4.3 `PURCHASE` message: gateway looks up the item and the player's current balance; if insufficient,
    reject immediately without touching the ledger.
4.4 On sufficient balance: produce a `DEBIT` event to `economy-ledger` (mirrors the CREDIT flow from
    3.6–3.8); the economy consumer applies it against the in-memory balance.
4.5 On successful debit: add the item to the in-memory inventory, produce an
    `inventory-updated` event, gateway pushes updated balance + inventory to the client.
4.6 Frontend: a simple shop menu listing the catalog with prices and a buy button; on purchase, apply
    the cosmetic to the player's avatar sprite in the room scene (e.g. a hat overlay) and refresh the
    displayed balance.
4.7 Manual test: earn currency via the word game, buy an item, see it equipped in the room view;
    attempt a purchase you can't afford and confirm it's rejected with no balance change.

---

Once you're happy with this breakdown, next steps would be either: start on 1.1 (repo scaffolding), or
sketch the shared schema package (2.3/2.9, 3.2, 3.6) first if you'd rather nail all the message
contracts before writing gateway code. Your call.

# Implementation Plan

This supplements [cozy_critter_context.md](cozy_critter_context.md) (the PRD) with the finer-grained,
decided steps we're actually building against. Update this file as decisions change.

## Status

- **Milestone 1 (Local Loopback Foundation):** 1.1–1.5 done. 1.6 (Makefile bootstrap) and 1.7 (CI
  stretch) intentionally skipped for now — jumped to Milestone 2 once the loopback was proven working
  end to end in a real browser. Revisit 1.6/1.7 opportunistically later.
- **Milestone 2 (Room Sync):** 2.1–2.2 done (private code-joined rooms). Movement (2.3+) is next.
- **Milestones 3–4:** not started.

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
- **No sprite/background art style decided yet.** Use simple placeholder shapes (colored circles/
  rectangles, maybe a label) for critters and rooms in the meantime. Load sprites from a small config/
  manifest (critter type → texture key) rather than hardcoding shapes inline in scene code, so that
  real art (commissioned from a friend, or a self-made early pass) is a data/asset swap later, not a
  rendering-logic rewrite. Relevant starting at Milestone 2 step 2.7.

## Open / deferred (not blocking the next milestones)

- Live cloud deployment: undecided. Local + kind + strong docs is enough for now; revisit once the
  core loop works.
- Full chat moderation system (rate limiting, reporting, mutes/bans).
- Multi-instance gateway scaling design.
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
1.6 ⏭️ skipped for now — One-command bootstrap (`Makefile` or script): `make up` builds and starts the
    whole Compose stack.
1.7 ⏭️ skipped for now (stretch) — GitHub Actions CI: `go vet` + `go test` on push.

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
2.3 Define the `player-positions` payload as Go structs matching the PRD schema; put them in a shared
    `internal/schema` package so gateway and any future consumers agree on the shape.
2.4 Create the `player-positions` Kafka topic, partitioned by `room_id`.
2.5 On a `MOVE` message from a joined client: validate shape, stamp `event_id`/`timestamp` server-side,
    produce to `player-positions` keyed by `room_id`.
2.6 Broadcast path: since the gateway is a single instance, the simplest correct approach is to consume
    `player-positions` in-process and fan out directly to the room's connections (no need for a second
    hop yet) — document this as the seam where multi-instance fan-out would later insert a real
    consumer-group broadcast step.
2.7 Frontend: Phaser scene renders a simple grid room; other players are sprites; on a broadcast MOVE
    event, tween the corresponding sprite toward the target position.
2.8 Two-tab manual test: join both tabs to the same room code, move in Tab A, confirm Tab B updates.

**Chat (stub pipeline)**
2.9 Define `chat-messages` payload as Go structs (mirrors PRD schema), create the topic.
2.10 On a `CHAT` message from a joined client: produce to `chat-messages` with `status:
     PENDING_VALIDATION`.
2.11 Stub filter consumer: consumes `chat-messages`, checks against a small hardcoded wordlist, marks
     `APPROVED` or `REJECTED`, and for `APPROVED` messages triggers the same broadcast path as movement
     (scoped to the message's `room_id`).
2.12 Frontend: minimal chat log overlay in the room scene; sending a message and seeing it (or a
     same-tab echo of a rejected one) appear.
2.13 Two-tab manual test: chat from Tab A appears in Tab B; a wordlist-flagged message is visibly
     rejected.

**Kubernetes parity**
2.14 Containerize the gateway (`Dockerfile`).
2.15 Stand up a local `kind` cluster.
2.16 Install Strimzi operator into `kind`; define a `Kafka` custom resource for a single-broker cluster
     (mirrors the Compose setup, not a production topology).
2.17 Write k8s manifests (Deployment + Service) for the gateway; point it at the Strimzi-managed broker.
2.18 Re-run the Milestone 2 two-tab tests (movement + chat) against the `kind`-deployed stack instead
     of Compose, to confirm parity.

## Milestone 3: Cheat-Proof Word Game & Economy Loop

Goal: a player plays a Wordle-style game entirely validated server-side, and a win credits their
(ephemeral, in-memory) currency balance — proving the puzzle-to-economy pipeline end to end.

**Word game**
3.1 Curate a word list bundled with the backend (a fixed answer list + a broader allowed-guesses list,
    Wordle-style, so players can't be blocked by "not a real word" on legitimate guesses).
3.2 Define `game-sessions` payload as Go structs (session_id, player_id, word length, guesses,
    status) in the shared schema package; create the `game-sessions` topic.
3.3 `START_GAME` message: gateway picks a random target word server-side (never sent to the client),
    creates a session, produces a session-started event to `game-sessions`, responds to the client with
    just `session_id` + guesses-remaining.
3.4 `GUESS` message: gateway validates the guess is in the allowed-guesses list, computes per-letter
    feedback (correct/present/absent) against the server-held target word, appends to session state,
    produces a guess-evaluated event, and returns the feedback to the client (not room-broadcast — this
    is single-player, private to that session).
3.5 Win/loss detection: on a correct guess or exhausted attempts, mark the session complete and produce
    a session-completed event.

**Economy (ephemeral)**
3.6 Define `economy-ledger` payload as Go structs matching the PRD schema (transaction_id, player_id,
    action CREDIT/DEBIT, amount, verification data) — build the real-looking schema even though storage
    is in-memory for now (see "Locked-in decisions").
3.7 On session-completed (win), produce a `CREDIT` event sized by guesses used (e.g. fewer guesses =
    more currency).
3.8 Economy consumer: consumes `economy-ledger`, applies CREDIT/DEBIT against an in-memory
    `map[player_id]balance` (guard with a mutex or a single-consumer-per-key pattern to avoid races),
    pushes a `currency-balance-updated` event back to the client.
3.9 Frontend: word-game UI (letter grid, on-screen keyboard, color feedback), currency balance visible
    and ticking up on a win.
3.10 Manual test: play a full game to a win, confirm balance increases by the expected amount; play a
     losing game, confirm no credit.

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

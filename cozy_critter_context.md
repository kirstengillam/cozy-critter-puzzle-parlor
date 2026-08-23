# Product Requirements Document (PRD) & Architecture Context
**Project Name:** The Cozy Critter Puzzle Parlor
**Target System Compatibility:** Claude Code / AI Developer Context

---

## 1. Executive Summary & Purpose
The Cozy Critter Puzzle Parlor is a remote, self-contained, event-driven multiplayer social MMO and casual brain-training platform. Inspired by the community mechanics of Club Penguin and the accessibility of modern web games, this platform allows players to select cute animal avatars (capybaras, frogs, ducks, platypuses), customize their outfits, chat in real-time within persistent virtual environments ("rooms"), and play casual games (Sudoku, word games, turn-based multiplayer board games).

### Core Goals
* **Invisible Frontend / Heavy Backend:** Mitigate minimalist or "crude" frontend engineering constraints by building a high-performance, robust, and cheat-proof backend engine.
* **Scalable Event-Driven Core:** Leverage Apache Kafka to handle real-time message streaming, room state synchronization, cheat-proof economy transactions, and multiplayer matchmaking.
* **Self-Sustaining Economy:** Provide an automated transactional loop where players earn currency natively via validated mini-games and spend it on cosmetic asset customizations.

**Distribution note:** Public, zero-outreach distribution (itch.io, CrazyGames, cozy gaming communities)
is a possible path, not a committed core goal — this may instead end up self-hosted (e.g. for a private
group of known players) rather than publicly distributed. Architecture and moderation decisions should
not assume public discovery is guaranteed.

---

## 2. Core Functional Requirements

### A. Real-Time Multi-User Environment (The Parlor)
* **Persistent Rooms:** Multiple thematic, grid-based layout areas (e.g., Lounge, Library, Hot Springs) where players co-exist.
* **Real-Time Synchronous State:** Players can see other avatars move, update outfits, and broadcast chat messages instantly.
* **Automated In-Game Moderation:** Backend-driven ingestion pipelines to sanitize text and manage user states.

### B. Mini-Game Portfolio & Validation Loop
* **Single-Player Brain Teasers:** Integrated puzzles like Sudoku, Wordle clones, and typing-speed metrics.
* **Backend Verification:** To prevent client-side memory injection or API tampering, puzzle generation, move validation, and final scores are completely executed and checked by backend microservices.
* **Multiplayer Turn-Based Games:** Interactive "game tables" within rooms allowing players to initiate 2-player games like Scrabble or chess.

### C. Cosmetic Customization & Economy
* **Niche Avatars & Items:** Currency earned from puzzles is sent to a central wallet to purchase hats, shirts, backgrounds, and trail effects.
* **No UI Checkout Panels:** Inventory purchasing is processed via text-commands, automated interaction nodes, or simple menu layouts, shifting the engineering focus onto database transaction atomicity.

---

## 3. High-Level Architectural Design

The platform uses an **event-driven microservices architecture** with **Apache Kafka** serving as the central log and streaming backbone. Communication between clients and the backend runs over persistent, low-latency **WebSockets**.

```
[ Player Client (HTML5 / Phaser.js / Canvas) ]
                     │
                     ▼ (WebSockets / JSON)
        [ API & WebSocket Gateway Layer ]
                     │
                     ▼ (Produce Events)
             ───────[ KAFKA CORE ]───────
                     │
     ┌───────────────┼───────────────┬───────────────┐
     ▼               ▼               ▼               ▼
[Room Service] [Chat Filter]  [Game Engine]  [Economy Ledger]
     │               │               │               │
     └───────────────┴───────┬───────┴───────────────┘
                             │ (Consume & Broadcast)
                             ▼
               [ WebSocket Gateway Layer ]
                             │
                             ▼ (WebSockets / JSON)
            [ All Impacted Player Clients ]
```

### Component Breakdown
1. **Frontend Client:** Lightweight canvas/HTML5 wrapper (e.g., Phaser.js or raw WebSockets) that displays basic 2D assets, catches user inputs, and maps coordinates.
2. **WebSocket Gateway:** Stateless ingress nodes that maintain open TCP connections with users, translate incoming WebSockets payloads into Kafka events, and push processed Kafka messages back out to clients.
3. **Kafka Event Bus:** The immutable streaming backbone storing logs for auditing, room state, match actions, and currency transactions.
4. **Backend Worker Services:** Isolated consumer groups managing specific business logic domains (Room Routing, AI Chat Sanitation, Game Validation, Ledger Database Persistence).

---

## 4. Technical Specifications & Data Schemas

### A. Kafka Topic Topology
* `player-positions`: Partitioned by `room_id`. Captures high-volume spatial updates.
* `chat-messages`: Multi-partitioned topic routing public messages through filtering engines before delivery.
* `game-sessions`: Tracking state changes, initial matrices (Sudoku), and step-by-step validations.
* `economy-ledger`: High-priority, transactional topic recording double-entry bookkeeping events.

### B. Critical Data Payloads (JSON)

#### 1. Player Movement Event (`player-positions` topic)
```json
{
  "event_id": "uuid-v4-spatial-1102",
  "timestamp": 1787498400000,
  "player_id": "player_capy_99",
  "room_id": "cozy_lounge_01",
  "action": "MOVE",
  "payload": {
    "current_x": 142,
    "current_y": 85,
    "target_x": 150,
    "target_y": 90,
    "facing_direction": "NORTH_EAST"
  }
}
```

#### 2. Chat Ingestion & Filtering Payload (`chat-messages` topic)
```json
{
  "message_id": "uuid-v4-msg-5501",
  "timestamp": 1787498405000,
  "player_id": "player_frog_02",
  "room_id": "cozy_lounge_01",
  "raw_text": "Hey everyone! Come play Sudoku at Table 3!",
  "status": "PENDING_VALIDATION"
}
```

#### 3. Secure Mini-Game Reward Event (`economy-ledger` topic)
```json
{
  "transaction_id": "tx-uuid-88392",
  "timestamp": 1787498420000,
  "player_id": "player_capy_99",
  "game_session_id": "sudoku-session-xyz",
  "action": "CREDIT",
  "amount": 25,
  "currency_type": "CRITTER_COINS",
  "verification_hash": "sha256-signature-generated-by-backend-validator"
}
```

---

## 5. Development Milestones & Roadmap
1. **Milestone 1 (Local Setup):** Spin up Docker Compose containing a single KRaft Kafka broker, a basic Python/Go WebSocket server, and a static index.html client to verify low-latency loopback echo.
2. **Milestone 2 (Room Synchronization):** Implement the `player-positions` schema. Confirm that when Tab A updates its coordinates, Tab B automatically renders the new coordinates over a local state representation.
3. **Milestone 3 (The Cheat-Proof Puzzle):** Build a basic word/Sudoku backend generation engine. Require the client to submit answers to the socket, verify it via consumer groups, and increment a mock account database store.
4. **Milestone 4 (Cosmetic Customization):** Create a database schema for user profiles, inventory state arrays, and store item records. Validate item purchases securely across transactions.

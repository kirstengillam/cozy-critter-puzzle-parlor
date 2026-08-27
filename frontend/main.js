import {
  DEFAULT_CRITTER,
  animKey,
  createCritterSprite,
  preloadCritterTextures,
  registerCritterAnimations,
} from "./critters.js";

const WS_PORT = 8080;
const GRID_SIZE = 32;
const GRID_COLS = 16;
const GRID_ROWS = 9;
// Per-cell move time, not a flat per-move duration — there's no
// pathfinding, so a click can land many cells away in a straight line,
// and a flat duration made short hops crawl while long hops teleported.
// At 200ms, the walking animation (8 frames @ 10fps = 800ms/cycle) barely
// got 2 frames in before snapping back to idle — slowed down so the walk
// cycle actually reads as walking.
const MOVE_MS_PER_CELL = 550;

// Displays the whole scene (background, critters, labels) at 2x CSS size
// via Phaser's scale-manager zoom, without changing any grid/movement math
// below — the game's internal coordinate space stays GRID_COLS*GRID_SIZE.
const ZOOM = 2;

// Native cat sprites are 64x64. Scaled down to a bit larger than the 32px
// grid cell on purpose (cozy chibi-style oversized avatars, not a strict
// one-cell fit) — LABEL_OFFSET tracks half the resulting sprite height so
// the name label still sits right at its feet.
const CRITTER_SCALE = 0.7;
const LABEL_OFFSET = 22;

// Speech bubble shown above a player's sprite for each chat message,
// alongside the persistent lobby chat log. TAIL_TIP_OFFSET is how far
// above the sprite's center the tail's point sits; the rounded body is
// drawn further up from there, sized to fit the text.
const BUBBLE_TAIL_TIP_OFFSET = 26;
const BUBBLE_TAIL_WIDTH = 10;
const BUBBLE_TAIL_HEIGHT = 6;
const BUBBLE_PADDING_X = 6;
const BUBBLE_PADDING_Y = 4;
const BUBBLE_CORNER_RADIUS = 6;
const BUBBLE_DURATION_MS = 4000;
const BUBBLE_WRAP_WIDTH = 110;

// Room background art is a 256x144 placeholder; scaling it 2x fills the
// GRID_COLS x GRID_ROWS canvas exactly (16*32 x 9*32 = 512x288) without
// touching the critter sprite scale tuned above.
const BACKGROUND_SCALE = 2;

// Each visual table in the background art is a spot players can walk onto
// to start a game. Cell coords were derived from the background image's
// table blob centroids, scaled by BACKGROUND_SCALE. gameType lets future
// tables launch something other than the word game without touching the
// movement/arrival plumbing below.
const GAME_TABLES = [
  { id: "table-a", cell: { x: 8, y: 2 }, gameType: "word" },
  { id: "table-b", cell: { x: 3, y: 4 }, gameType: "word" },
  { id: "table-c", cell: { x: 10, y: 4 }, gameType: "connections" },
];

function findTableAt(x, y) {
  return GAME_TABLES.find((t) => t.cell.x === x && t.cell.y === y) ?? null;
}

function startGameAtTable(table) {
  if (table.gameType === "connections") {
    send("START_CONNECTIONS", { player_id: playerId });
  } else {
    send("START_GAME", { player_id: playerId });
  }
}

const playerId = "player_" + Math.random().toString(36).slice(2, 8);
const fallbackDisplayName = playerId.replace(/^player_/, "");

let socket;
let roomCode = null;
let scene = null;
let pendingGameTable = null;
const sprites = {}; // player_id -> { image: Phaser.GameObjects.Image, label: Phaser.GameObjects.Text }

const statusEl = document.getElementById("status");
const displayNameInputEl = document.getElementById("display-name-input");
displayNameInputEl.value = fallbackDisplayName;

// The name to attach to this player's own JOIN_ROOM/moves/chat: whatever
// is currently typed in the box, or the auto-generated fallback if it's
// been cleared.
function currentDisplayName() {
  return displayNameInputEl.value.trim() || fallbackDisplayName;
}

const roomCodeInput = document.getElementById("room-code-input");
const roomEl = document.getElementById("room");
const chatLogEl = document.getElementById("chat-log");
const chatInputEl = document.getElementById("chat-input");
const balanceAmountEl = document.getElementById("balance-amount");
const wordgameEl = document.getElementById("wordgame");
const wordGridEl = document.getElementById("word-grid");
const keyboardEl = document.getElementById("keyboard");
const wordgameStatusEl = document.getElementById("wordgame-status");

const connectionsEl = document.getElementById("connections");
const connGridEl = document.getElementById("conn-grid");
const connSolvedEl = document.getElementById("conn-solved");
const connMistakesEl = document.getElementById("conn-mistakes");
const connSubmitEl = document.getElementById("conn-submit");
const connectionsStatusEl = document.getElementById("connections-status");

function setStatus(text) {
  statusEl.textContent = text;
}

function send(type, payload) {
  socket.send(JSON.stringify({ type, payload: payload ?? {} }));
}

function wsUrl() {
  if (location.protocol === "https:") {
    return `wss://${location.host}/ws`;
  }
  return `ws://${location.hostname}:${WS_PORT}/ws`;
}

function connect() {
  socket = new WebSocket(wsUrl());
  socket.addEventListener("open", () => setStatus(`connected as ${playerId}`));
  socket.addEventListener("close", () => setStatus("disconnected"));
  socket.addEventListener("error", () => setStatus("connection error"));
  socket.addEventListener("message", handleMessage);
}

function handleMessage(event) {
  const env = JSON.parse(event.data);
  switch (env.type) {
    case "ROOM_CREATED":
      roomCodeInput.value = env.payload.room_code;
      send("JOIN_ROOM", { player_id: playerId, room_code: env.payload.room_code, display_name: currentDisplayName() });
      break;
    case "JOINED":
      roomCode = env.payload.room_code;
      setStatus(`joined room ${roomCode} as ${currentDisplayName()}`);
      roomEl.style.display = "block";
      break;
    case "PLAYER_MOVED":
      onPlayerMoved(env.payload);
      break;
    case "CHAT_MESSAGE":
      appendChatLine(`${env.payload.display_name || env.payload.player_id}: ${env.payload.raw_text}`, false);
      showChatBubble(env.payload.player_id, env.payload.raw_text);
      break;
    case "CHAT_REJECTED":
      appendChatLine(`(your message was rejected: "${env.payload.raw_text}")`, true);
      break;
    case "GAME_STARTED":
      onGameStarted(env.payload);
      break;
    case "GUESS_RESULT":
      onGuessResult(env.payload);
      break;
    case "CONNECTIONS_STARTED":
      onConnectionsStarted(env.payload);
      break;
    case "CONNECTIONS_RESULT":
      onConnectionsResult(env.payload);
      break;
    case "CURRENCY_BALANCE_UPDATED":
      balanceAmountEl.textContent = env.payload.balance;
      break;
    case "ERROR":
      setStatus(`error: ${env.payload.message}`);
      break;
  }
}

function appendChatLine(text, rejected) {
  const p = document.createElement("p");
  p.textContent = text;
  if (rejected) p.className = "rejected";
  chatLogEl.appendChild(p);
  chatLogEl.scrollTop = chatLogEl.scrollHeight;
}

// Pops a temporary speech bubble above playerId's sprite. No-ops if that
// player doesn't have a sprite yet (they haven't moved since joining) —
// same constraint the name label already has. Position is kept in sync
// with the sprite every frame (see the scene's update function below),
// so it follows the player through any in-progress movement tween.
function showChatBubble(playerId, text) {
  const entry = sprites[playerId];
  if (!entry || !scene) return;

  if (entry.bubbleTimer) entry.bubbleTimer.remove(false);
  if (entry.bubble) entry.bubble.destroy();

  // Text is measured unattached first so the rounded body can be sized
  // to fit it, then both are combined into one container.
  const label = scene.add
    .text(0, 0, text, {
      fontSize: "9px",
      color: "#222222",
      wordWrap: { width: BUBBLE_WRAP_WIDTH },
      align: "center",
    })
    .setOrigin(0.5, 0.5);

  const bodyWidth = label.width + BUBBLE_PADDING_X * 2;
  const bodyHeight = label.height + BUBBLE_PADDING_Y * 2;
  const bodyLeft = -bodyWidth / 2;
  const bodyTop = -BUBBLE_TAIL_HEIGHT - bodyHeight;

  const bg = scene.add.graphics();
  bg.fillStyle(0xffffff, 1);
  bg.lineStyle(1, 0x333333, 1);
  bg.fillRoundedRect(bodyLeft, bodyTop, bodyWidth, bodyHeight, BUBBLE_CORNER_RADIUS);
  // The tail's base overlaps the body slightly so the seam between the
  // two shapes doesn't show through the body's stroke.
  bg.fillTriangle(
    -BUBBLE_TAIL_WIDTH / 2,
    -BUBBLE_TAIL_HEIGHT + 1,
    BUBBLE_TAIL_WIDTH / 2,
    -BUBBLE_TAIL_HEIGHT + 1,
    0,
    0,
  );
  bg.strokeRoundedRect(bodyLeft, bodyTop, bodyWidth, bodyHeight, BUBBLE_CORNER_RADIUS);

  label.setPosition(0, bodyTop + bodyHeight / 2);

  const container = scene.add.container(entry.image.x, entry.image.y - BUBBLE_TAIL_TIP_OFFSET, [bg, label]);
  entry.bubble = container;

  entry.bubbleTimer = scene.time.delayedCall(BUBBLE_DURATION_MS, () => {
    entry.bubble?.destroy();
    entry.bubble = null;
    entry.bubbleTimer = null;
  });
}

function cellCenter(cellX, cellY) {
  return { x: cellX * GRID_SIZE + GRID_SIZE / 2, y: cellY * GRID_SIZE + GRID_SIZE / 2 };
}

function onPlayerMoved(evt) {
  if (!scene) return;

  let entry = sprites[evt.player_id];
  if (!entry) {
    const start = cellCenter(evt.current_x, evt.current_y);
    const image = createCritterSprite(scene, DEFAULT_CRITTER, start.x, start.y);
    image.setScale(CRITTER_SCALE);
    const name = evt.player_id === playerId ? currentDisplayName() : evt.display_name || evt.player_id.replace(/^player_/, "");
    const label = scene.add
      .text(start.x, start.y + LABEL_OFFSET, name, { fontSize: "9px", color: "#ffffff" })
      .setOrigin(0.5, 0);
    entry = { image, label };
    sprites[evt.player_id] = entry;
  }

  const target = cellCenter(evt.target_x, evt.target_y);
  const dx = target.x - entry.image.x;
  const dy = target.y - entry.image.y;
  const distance = Math.hypot(dx, dy);

  if (distance > 0) {
    // Sheet faces right by default; flip to face the direction of travel.
    if (dx !== 0) entry.image.setFlipX(dx < 0);
    entry.image.play(animKey(DEFAULT_CRITTER, "walking"), true);

    const duration = (distance / GRID_SIZE) * MOVE_MS_PER_CELL;
    scene.tweens.add({
      targets: entry.image,
      x: target.x,
      y: target.y,
      duration,
      onComplete: () => entry.image.play(animKey(DEFAULT_CRITTER, "idle"), true),
    });
    scene.tweens.add({
      targets: entry.label,
      x: target.x,
      y: target.y + LABEL_OFFSET,
      duration,
    });
  }

  if (
    evt.player_id === playerId &&
    pendingGameTable &&
    evt.target_x === pendingGameTable.cell.x &&
    evt.target_y === pendingGameTable.cell.y
  ) {
    const table = pendingGameTable;
    pendingGameTable = null;
    startGameAtTable(table);
  }
}

const KEYBOARD_ROWS = ["qwertyuiop", "asdfghjkl", "zxcvbnm"];

let wordGameSessionId = null;
let wordGameWordLength = 0;
let wordGameMaxGuesses = 0;
let wordGameRow = 0;
let currentGuess = "";
const keyStates = {}; // letter -> best-known state (correct > present > absent)

function buildWordGrid(maxGuesses, wordLength) {
  wordGridEl.innerHTML = "";
  for (let r = 0; r < maxGuesses; r++) {
    const row = document.createElement("div");
    row.className = "word-row";
    for (let c = 0; c < wordLength; c++) {
      const tile = document.createElement("div");
      tile.className = "tile";
      row.appendChild(tile);
    }
    wordGridEl.appendChild(row);
  }
}

function makeKey(label, wide) {
  const btn = document.createElement("button");
  btn.textContent = label === "BACK" ? "⌫" : label.toUpperCase();
  btn.className = "kb-key" + (wide ? " wide" : "");
  btn.addEventListener("click", () => onWordGameKey(label));
  if (label.length === 1) btn.dataset.letter = label;
  return btn;
}

function buildKeyboard() {
  keyboardEl.innerHTML = "";
  for (const key of Object.keys(keyStates)) delete keyStates[key];
  KEYBOARD_ROWS.forEach((rowStr, i) => {
    const row = document.createElement("div");
    row.className = "kb-row";
    if (i === 2) row.appendChild(makeKey("ENTER", true));
    for (const ch of rowStr) row.appendChild(makeKey(ch, false));
    if (i === 2) row.appendChild(makeKey("BACK", true));
    keyboardEl.appendChild(row);
  });
}

function renderCurrentRow() {
  const row = wordGridEl.children[wordGameRow];
  if (!row) return;
  for (let i = 0; i < wordGameWordLength; i++) {
    row.children[i].textContent = currentGuess[i] ?? "";
  }
}

const STATE_RANK = { absent: 0, present: 1, correct: 2 };

function onWordGameKey(key) {
  if (!wordGameSessionId) return;
  if (key === "ENTER") {
    if (currentGuess.length !== wordGameWordLength) return;
    send("GUESS", { session_id: wordGameSessionId, guess: currentGuess });
    return;
  }
  if (key === "BACK") {
    currentGuess = currentGuess.slice(0, -1);
    renderCurrentRow();
    return;
  }
  if (currentGuess.length < wordGameWordLength) {
    currentGuess += key;
    renderCurrentRow();
  }
}

document.addEventListener("keydown", (e) => {
  if (!wordGameSessionId) return;
  // Don't hijack typing into the room-code or chat inputs.
  if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
  if (e.key === "Enter") onWordGameKey("ENTER");
  else if (e.key === "Backspace") onWordGameKey("BACK");
  else if (/^[a-zA-Z]$/.test(e.key)) onWordGameKey(e.key.toLowerCase());
});

function onGameStarted(payload) {
  connectionsSessionId = null;
  connectionsEl.style.display = "none";

  wordGameSessionId = payload.session_id;
  wordGameWordLength = payload.word_length;
  wordGameMaxGuesses = payload.guesses_remaining;
  wordGameRow = 0;
  currentGuess = "";
  wordgameStatusEl.textContent = "";
  buildWordGrid(wordGameMaxGuesses, wordGameWordLength);
  buildKeyboard();
  wordgameEl.style.display = "block";
}

function onGuessResult(payload) {
  const row = wordGridEl.children[wordGameRow];
  payload.feedback.forEach((f, i) => {
    const tile = row.children[i];
    tile.textContent = f.letter;
    tile.classList.add(f.state.toLowerCase());

    const letter = f.letter.toLowerCase();
    const state = f.state.toLowerCase();
    if (STATE_RANK[state] > (STATE_RANK[keyStates[letter]] ?? -1)) {
      keyStates[letter] = state;
      const keyBtn = keyboardEl.querySelector(`[data-letter="${letter}"]`);
      if (keyBtn) {
        keyBtn.classList.remove("correct", "present", "absent");
        keyBtn.classList.add(state);
      }
    }
  });

  wordGameRow++;
  currentGuess = "";

  if (payload.status === "WON") {
    wordgameStatusEl.textContent = "You won! 🎉";
    wordGameSessionId = null;
  } else if (payload.status === "LOST") {
    wordgameStatusEl.textContent = "Out of guesses — better luck next time.";
    wordGameSessionId = null;
  }
}

let connectionsSessionId = null;
let connectionsSelected = []; // currently-selected words, max 4

function buildConnectionsGrid(words) {
  connGridEl.innerHTML = "";
  for (const word of words) {
    const tile = document.createElement("button");
    tile.className = "conn-tile";
    tile.textContent = word;
    tile.dataset.word = word;
    tile.addEventListener("click", () => toggleConnectionsTile(word));
    connGridEl.appendChild(tile);
  }
}

function toggleConnectionsTile(word) {
  const idx = connectionsSelected.indexOf(word);
  if (idx !== -1) {
    connectionsSelected.splice(idx, 1);
  } else if (connectionsSelected.length < 4) {
    connectionsSelected.push(word);
  }
  renderConnectionsSelection();
}

function renderConnectionsSelection() {
  for (const tile of connGridEl.children) {
    tile.classList.toggle("selected", connectionsSelected.includes(tile.dataset.word));
  }
  connSubmitEl.disabled = connectionsSelected.length !== 4;
}

function renderConnectionsMistakes(mistakesUsed, maxMistakes) {
  connMistakesEl.textContent = "Mistakes remaining: ";
  for (let i = 0; i < maxMistakes; i++) {
    const dot = document.createElement("span");
    dot.className = "conn-dot" + (i < mistakesUsed ? " used" : "");
    connMistakesEl.appendChild(dot);
  }
}

function renderConnectionsSolved(solvedGroups) {
  connSolvedEl.innerHTML = "";
  for (const g of solvedGroups) {
    const row = document.createElement("div");
    row.className = `conn-solved-row conn-level-${g.level}`;
    const name = document.createElement("span");
    name.className = "conn-solved-name";
    name.textContent = g.name;
    const members = document.createElement("span");
    members.className = "conn-solved-members";
    members.textContent = g.members.join(", ");
    row.append(name, members);
    connSolvedEl.appendChild(row);
  }
}

function onConnectionsStarted(payload) {
  wordGameSessionId = null;
  wordgameEl.style.display = "none";

  connectionsSessionId = payload.session_id;
  connectionsSelected = [];
  connectionsStatusEl.textContent = "";
  renderConnectionsSolved([]);
  buildConnectionsGrid(payload.words);
  renderConnectionsMistakes(0, payload.max_mistakes);
  renderConnectionsSelection();
  connectionsEl.style.display = "block";
}

function submitConnectionsGuess() {
  if (!connectionsSessionId || connectionsSelected.length !== 4) return;
  send("CONNECTIONS_GUESS", { session_id: connectionsSessionId, members: connectionsSelected });
}

function onConnectionsResult(payload) {
  renderConnectionsMistakes(payload.mistakes_used, payload.max_mistakes);
  renderConnectionsSolved(payload.solved_groups);
  connectionsStatusEl.textContent = "";

  if (payload.correct && payload.solved_group) {
    for (const word of payload.solved_group.members) {
      connGridEl.querySelector(`[data-word="${CSS.escape(word)}"]`)?.remove();
    }
  } else {
    for (const word of connectionsSelected) {
      const tile = connGridEl.querySelector(`[data-word="${CSS.escape(word)}"]`);
      if (!tile) continue;
      tile.classList.add("shake");
      setTimeout(() => tile.classList.remove("shake"), 300);
    }
    if (payload.one_away) connectionsStatusEl.textContent = "One away...";
  }
  connectionsSelected = [];
  renderConnectionsSelection();

  if (payload.status === "WON") {
    connectionsStatusEl.textContent = "Solved it! 🎉";
    connectionsSessionId = null;
  } else if (payload.status === "LOST") {
    connectionsStatusEl.textContent = "Out of guesses — better luck next time.";
    connectionsSessionId = null;
  }
}

// Temp placeholder room background (see IMPLEMENTATION_PLAN.md's
// "no sprite/background art style decided yet" note) — its 3 painted
// tables are the walkable GAME_TABLES cells above.
function drawBackground(scene) {
  scene.add.image(0, 0, "room-bg").setOrigin(0, 0).setScale(BACKGROUND_SCALE);
}

function drawGrid(scene) {
  const g = scene.add.graphics();
  g.lineStyle(1, 0xffffff, 0.15);
  for (let x = 0; x <= GRID_COLS; x++) {
    g.lineBetween(x * GRID_SIZE, 0, x * GRID_SIZE, GRID_ROWS * GRID_SIZE);
  }
  for (let y = 0; y <= GRID_ROWS; y++) {
    g.lineBetween(0, y * GRID_SIZE, GRID_COLS * GRID_SIZE, y * GRID_SIZE);
  }
}

document.getElementById("create-room").addEventListener("click", () => send("CREATE_ROOM", {}));
document.getElementById("join-room").addEventListener("click", () => {
  const code = roomCodeInput.value.trim().toUpperCase();
  if (!code) return;
  send("JOIN_ROOM", { player_id: playerId, room_code: code, display_name: currentDisplayName() });
});

function sendChat() {
  const text = chatInputEl.value.trim();
  if (!text || !roomCode) return;
  send("CHAT", { text });
  chatInputEl.value = "";
}
document.getElementById("chat-send").addEventListener("click", sendChat);
chatInputEl.addEventListener("keydown", (e) => {
  if (e.key === "Enter") sendChat();
});

document.getElementById("play-word-game").addEventListener("click", () => {
  send("START_GAME", { player_id: playerId });
});
document.getElementById("new-game").addEventListener("click", () => {
  send("START_GAME", { player_id: playerId });
});

document.getElementById("play-connections").addEventListener("click", () => {
  send("START_CONNECTIONS", { player_id: playerId });
});
document.getElementById("new-connections").addEventListener("click", () => {
  send("START_CONNECTIONS", { player_id: playerId });
});
connSubmitEl.addEventListener("click", submitConnectionsGuess);

connect();

new Phaser.Game({
  type: Phaser.AUTO,
  scale: {
    mode: Phaser.Scale.NONE,
    width: GRID_COLS * GRID_SIZE,
    height: GRID_ROWS * GRID_SIZE,
    zoom: ZOOM,
  },
  parent: "game",
  backgroundColor: "#2d2d3a",
  pixelArt: true, // crisp nearest-neighbor scaling for the pixel-art sprites
  scene: {
    preload: function () {
      this.load.image("room-bg", "assets/backgrounds/room-temp.png");
      preloadCritterTextures(this);
    },
    create: function () {
      scene = this;
      registerCritterAnimations(this);
      drawBackground(this);
      drawGrid(this);
      this.input.on("pointerdown", (pointer) => {
        if (!roomCode) return;
        // pointer.x/y go through Phaser's scale-manager transform, which
        // can come back Infinity/NaN in some hosting contexts before it
        // settles. event.offsetX/Y are canvas-relative straight from the
        // browser and don't depend on that transform — but they're in
        // rendered CSS pixels, which the ZOOM factor makes larger than the
        // internal game coordinate space, so divide it back out.
        const offsetX = (pointer.event?.offsetX ?? pointer.x) / ZOOM;
        const offsetY = (pointer.event?.offsetY ?? pointer.y) / ZOOM;
        const targetX = Math.floor(offsetX / GRID_SIZE);
        const targetY = Math.floor(offsetY / GRID_SIZE);
        const table = findTableAt(targetX, targetY);
        if (table) pendingGameTable = table;
        send("MOVE", { target_x: targetX, target_y: targetY, facing_direction: "NORTH" });
      });
    },
    update: function () {
      for (const entry of Object.values(sprites)) {
        entry.bubble?.setPosition(entry.image.x, entry.image.y - BUBBLE_TAIL_TIP_OFFSET);
      }
    },
  },
});

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
const MOVE_TWEEN_MS = 200;

// Displays the whole scene (background, critters, labels) at 2x CSS size
// via Phaser's scale-manager zoom, without changing any grid/movement math
// below — the game's internal coordinate space stays GRID_COLS*GRID_SIZE.
const ZOOM = 2;

// Native cat sprites are 64x64; scale them down to fit a 32px grid cell.
const CRITTER_SCALE = 0.5;
const LABEL_OFFSET = 16;

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
  { id: "table-c", cell: { x: 10, y: 4 }, gameType: "word" },
];

function findTableAt(x, y) {
  return GAME_TABLES.find((t) => t.cell.x === x && t.cell.y === y) ?? null;
}

// Only the word game exists today, so every table starts it the same way.
// Once more games exist, branch on table.gameType here.
function startGameAtTable(table) {
  send("START_GAME", { player_id: playerId });
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

function setStatus(text) {
  statusEl.textContent = text;
}

function send(type, payload) {
  socket.send(JSON.stringify({ type, payload: payload ?? {} }));
}

function connect() {
  socket = new WebSocket(`ws://${location.hostname}:${WS_PORT}/ws`);
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
  if (target.x !== entry.image.x) {
    // Sheet faces left by default; flip to face the direction of travel.
    entry.image.setFlipX(target.x > entry.image.x);
  }
  entry.image.play(animKey(DEFAULT_CRITTER, "walking"), true);
  scene.tweens.add({
    targets: entry.image,
    x: target.x,
    y: target.y,
    duration: MOVE_TWEEN_MS,
    onComplete: () => entry.image.play(animKey(DEFAULT_CRITTER, "idle"), true),
  });
  scene.tweens.add({
    targets: entry.label,
    x: target.x,
    y: target.y + LABEL_OFFSET,
    duration: MOVE_TWEEN_MS,
  });

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
  },
});

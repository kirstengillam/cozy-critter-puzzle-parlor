import { CRITTER_MANIFEST, registerCritterTextures } from "./critters.js";

const WS_PORT = 8080;
const GRID_SIZE = 32;
const GRID_COLS = 15;
const GRID_ROWS = 8;
const MOVE_TWEEN_MS = 200;

const playerId = "player_" + Math.random().toString(36).slice(2, 8);

let socket;
let roomCode = null;
let scene = null;
const sprites = {}; // player_id -> Phaser.GameObjects.Image

const statusEl = document.getElementById("status");
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
      send("JOIN_ROOM", { player_id: playerId, room_code: env.payload.room_code });
      break;
    case "JOINED":
      roomCode = env.payload.room_code;
      setStatus(`joined room ${roomCode} as ${playerId}`);
      roomEl.style.display = "block";
      break;
    case "PLAYER_MOVED":
      onPlayerMoved(env.payload);
      break;
    case "CHAT_MESSAGE":
      appendChatLine(`${env.payload.player_id}: ${env.payload.raw_text}`, false);
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

  let sprite = sprites[evt.player_id];
  if (!sprite) {
    const start = cellCenter(evt.current_x, evt.current_y);
    sprite = scene.add.image(start.x, start.y, CRITTER_MANIFEST.default.textureKey);
    sprites[evt.player_id] = sprite;
  }

  const target = cellCenter(evt.target_x, evt.target_y);
  scene.tweens.add({ targets: sprite, x: target.x, y: target.y, duration: MOVE_TWEEN_MS });
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

function drawGrid(scene) {
  const g = scene.add.graphics();
  g.lineStyle(1, 0x44445a, 1);
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
  send("JOIN_ROOM", { player_id: playerId, room_code: code });
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
  width: GRID_COLS * GRID_SIZE,
  height: GRID_ROWS * GRID_SIZE,
  parent: "game",
  backgroundColor: "#2d2d3a",
  scene: {
    create: function () {
      scene = this;
      registerCritterTextures(this);
      drawGrid(this);
      this.input.on("pointerdown", (pointer) => {
        if (!roomCode) return;
        // pointer.x/y go through Phaser's scale-manager transform, which
        // can come back Infinity/NaN in some hosting contexts before it
        // settles. event.offsetX/Y are canvas-relative straight from the
        // browser and don't depend on that transform.
        const offsetX = pointer.event?.offsetX ?? pointer.x;
        const offsetY = pointer.event?.offsetY ?? pointer.y;
        const targetX = Math.floor(offsetX / GRID_SIZE);
        const targetY = Math.floor(offsetY / GRID_SIZE);
        send("MOVE", { target_x: targetX, target_y: targetY, facing_direction: "NORTH" });
      });
    },
  },
});

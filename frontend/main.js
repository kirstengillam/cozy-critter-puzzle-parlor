import {
  CRITTER_MANIFEST,
  DEFAULT_CRITTER,
  animKey,
  createCritterSprite,
  preloadCritterTextures,
  registerCritterAnimations,
} from "./critters.js";

const WS_PORT = 8080;
const GRID_SIZE = 32;
// 12x11 keeps the room close to square (matches the approved "Parlor"
// mockup's ~1.1:1 frame) instead of the old 16:9 wide rectangle.
const GRID_COLS = 12;
const GRID_ROWS = 11;
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

// "The Parlor" room: a wallpapered back wall (rows 0-2) with a fireplace,
// and a floor (rows 3-10) centered on a rug. Each entry is a piece of
// furniture players walk onto to start that game — positions chosen to
// mirror the approved mockup's symmetric layout (card table centered on
// the rug, desk and side table flanking it in the front corners).
const GAME_TABLES = [
  { id: "writing-desk", cell: { x: 2, y: 8 }, gameType: "word" },
  { id: "card-table", cell: { x: 6, y: 6 }, gameType: "connect_four" },
  { id: "side-table", cell: { x: 9, y: 8 }, gameType: "connections" },
];

function findTableAt(x, y) {
  return GAME_TABLES.find((t) => t.cell.x === x && t.cell.y === y) ?? null;
}

function startGameAtTable(table) {
  if (table.gameType === "connections") {
    send("START_CONNECTIONS", { player_id: playerId });
  } else if (table.gameType === "connect_four") {
    send("START_CONNECT_FOUR", { player_id: playerId });
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

// Builds one clickable swatch per CRITTER_MANIFEST color, each showing
// that critter's own sitting-sheet frame 0 (a 320x64 5-frame strip, so a
// 32px-tall swatch needs the sheet at half size — 160x32 — to land frame
// 0 exactly in the button's top-left corner via background-position 0 0).
const critterPickerEl = document.getElementById("critter-picker");
let selectedCritter = DEFAULT_CRITTER;

function currentCritter() {
  return selectedCritter;
}

function selectCritter(critter) {
  selectedCritter = critter;
  for (const btn of critterPickerEl.children) {
    btn.classList.toggle("selected", btn.dataset.critter === critter);
  }
}

function buildCritterPicker() {
  for (const critter of Object.keys(CRITTER_MANIFEST)) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "critter-option";
    btn.dataset.critter = critter;
    btn.title = critter;
    btn.setAttribute("role", "radio");
    btn.style.setProperty("--critter-swatch", `url(${CRITTER_MANIFEST[critter].sitting.path})`);
    btn.addEventListener("click", () => selectCritter(critter));
    critterPickerEl.appendChild(btn);
  }
  selectCritter(selectedCritter);
}
buildCritterPicker();

const roomCodeInput = document.getElementById("room-code-input");
const roomEl = document.getElementById("room");
const roomCodeLabelEl = document.getElementById("room-code-label");
const roomBalanceAmountEl = document.getElementById("room-balance-amount");
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

const connectFourEl = document.getElementById("connectfour");
const c4BoardEl = document.getElementById("c4-board");
const c4StatusEl = document.getElementById("c4-status");

// Each of wordgame/connections/connectfour normally lives in #play-area
// (their "standalone" home, used for solo word game/Connections started
// straight from the lobby bar before ever joining a room — untouched,
// still unstyled pending the lobby's own redesign pass). Once in a room,
// starting any of them instead moves that same element into the room's
// game-frame in place of the Phaser canvas, so chat stays reachable the
// whole time. Connect Four never has a standalone home — it's only ever
// reachable by walking to a table inside a room.
const playAreaEl = document.getElementById("play-area");
const gameCanvasEl = document.getElementById("game");
const gamePanelHostEl = document.getElementById("game-panel-host");
const gamePanelTitleEl = document.getElementById("game-panel-title");
const gamePanelBodyEl = document.getElementById("game-panel-body");
const gamePanelBackEl = document.getElementById("game-panel-back");

function showGamePanel(panelEl, title) {
  panelEl.style.display = "block";
  if (roomCode) {
    gamePanelTitleEl.textContent = title;
    // gamePanelBodyEl should only ever hold the one active panel — evict
    // any other (hidden, but still DOM-attached) leftover from switching
    // games before it accumulates stale children.
    while (gamePanelBodyEl.firstElementChild && gamePanelBodyEl.firstElementChild !== panelEl) {
      playAreaEl.appendChild(gamePanelBodyEl.firstElementChild);
    }
    gamePanelBodyEl.appendChild(panelEl);
    gamePanelHostEl.style.display = "flex";
    gameCanvasEl.style.display = "none";
  } else {
    playAreaEl.appendChild(panelEl);
    // The lobby screen is a full min-height:100vh card (see index.html),
    // so a solo game started from its buttons would otherwise render
    // entirely below the fold with no visual hint it opened at all.
    panelEl.scrollIntoView({ behavior: "instant", block: "start" });
  }
}

// Hides whichever game is currently active and, if it was showing inside
// the room's game-frame, moves it back out to its standalone home and
// restores the room-scene canvas.
function closeActiveGamePanel() {
  wordGameSessionId = null;
  wordgameEl.style.display = "none";
  connectionsSessionId = null;
  connectionsEl.style.display = "none";
  closeConnectFour();

  if (gamePanelBodyEl.firstElementChild) {
    playAreaEl.appendChild(gamePanelBodyEl.firstElementChild);
  }
  gamePanelHostEl.style.display = "none";
  gameCanvasEl.style.display = "";
}
gamePanelBackEl.addEventListener("click", closeActiveGamePanel);

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
      send("JOIN_ROOM", {
        player_id: playerId,
        room_code: env.payload.room_code,
        display_name: currentDisplayName(),
        critter_type: currentCritter(),
      });
      break;
    case "JOINED":
      roomCode = env.payload.room_code;
      setStatus(`joined room ${roomCode} as ${currentDisplayName()}`);
      roomCodeLabelEl.textContent = roomCode;
      roomEl.style.display = "block";
      // The room view replaces the lobby as its own full screen (see
      // index.html's body.in-room rules) rather than sitting as a card
      // alongside it — matches the mockups, which were each a full
      // screen, background included, not a component embedded in a
      // larger page.
      document.body.classList.add("in-room");
      break;
    case "PLAYER_MOVED":
      onPlayerMoved(env.payload);
      break;
    case "PLAYER_LEFT":
      onPlayerLeft(env.payload);
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
    case "CONNECT_FOUR_WAITING":
      onConnectFourWaiting(env.payload);
      break;
    case "CONNECT_FOUR_STARTED":
      onConnectFourStarted(env.payload);
      break;
    case "CONNECT_FOUR_RESULT":
      onConnectFourResult(env.payload);
      break;
    case "CONNECT_FOUR_OPPONENT_LEFT":
      onConnectFourOpponentLeft(env.payload);
      break;
    case "CURRENCY_BALANCE_UPDATED":
      balanceAmountEl.textContent = env.payload.balance;
      roomBalanceAmountEl.textContent = env.payload.balance;
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
    const critter = evt.critter_type || DEFAULT_CRITTER;
    const start = cellCenter(evt.current_x, evt.current_y);
    const image = createCritterSprite(scene, critter, start.x, start.y);
    image.setScale(CRITTER_SCALE);
    const name = evt.player_id === playerId ? currentDisplayName() : evt.display_name || evt.player_id.replace(/^player_/, "");
    const label = scene.add
      .text(start.x, start.y + LABEL_OFFSET, name, { fontSize: "9px", color: "#ffffff" })
      .setOrigin(0.5, 0);
    entry = { image, label, critter };
    sprites[evt.player_id] = entry;
  }

  const target = cellCenter(evt.target_x, evt.target_y);
  const dx = target.x - entry.image.x;
  const dy = target.y - entry.image.y;
  const distance = Math.hypot(dx, dy);

  if (distance > 0) {
    // Sheet faces right by default; flip to face the direction of travel.
    if (dx !== 0) entry.image.setFlipX(dx < 0);
    entry.image.play(animKey(entry.critter, "walking"), true);

    // A new PLAYER_MOVED can arrive before the previous tween finishes
    // (e.g. a second tap mid-walk) — without killing the old tween first,
    // both fight over entry.image.x/y every frame and the sprite ends up
    // somewhere neither target intended.
    scene.tweens.killTweensOf(entry.image);
    scene.tweens.killTweensOf(entry.label);

    const duration = (distance / GRID_SIZE) * MOVE_MS_PER_CELL;
    scene.tweens.add({
      targets: entry.image,
      x: target.x,
      y: target.y,
      duration,
      onComplete: () => entry.image.play(animKey(entry.critter, "sitting"), true),
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

// Removes a disconnected player's sprite so it doesn't stay frozen in
// place forever for everyone still in the room.
function onPlayerLeft(payload) {
  const entry = sprites[payload.player_id];
  if (!entry) return;

  if (entry.bubbleTimer) entry.bubbleTimer.remove(false);
  if (entry.bubble) entry.bubble.destroy();
  entry.image.destroy();
  entry.label.destroy();
  delete sprites[payload.player_id];
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
  closeConnectFour();

  wordGameSessionId = payload.session_id;
  wordGameWordLength = payload.word_length;
  wordGameMaxGuesses = payload.guesses_remaining;
  wordGameRow = 0;
  currentGuess = "";
  wordgameStatusEl.textContent = "";
  buildWordGrid(wordGameMaxGuesses, wordGameWordLength);
  buildKeyboard();
  showGamePanel(wordgameEl, "Word Game");
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
  closeConnectFour();

  connectionsSessionId = payload.session_id;
  connectionsSelected = [];
  connectionsStatusEl.textContent = "";
  renderConnectionsSolved([]);
  buildConnectionsGrid(payload.words);
  renderConnectionsMistakes(0, payload.max_mistakes);
  renderConnectionsSelection();
  showGamePanel(connectionsEl, "Connections");
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

let c4SessionId = null;
let c4YourSymbol = null; // 1 or 2 (connectfour.PlayerOne/PlayerTwo)
let c4CurrentTurn = null; // player id whose turn it currently is, or null once the match is over

// Hides the Connect Four board and clears its session — called both
// when switching to a different game (see onGameStarted/
// onConnectionsStarted) and internally before showing a fresh board.
function closeConnectFour() {
  c4SessionId = null;
  c4YourSymbol = null;
  c4CurrentTurn = null;
  connectFourEl.style.display = "none";
}

function buildConnectFourBoard(board) {
  c4BoardEl.innerHTML = "";
  for (let r = 0; r < board.length; r++) {
    const rowEl = document.createElement("div");
    rowEl.className = "c4-row";
    for (let c = 0; c < board[r].length; c++) {
      const cell = document.createElement("div");
      cell.className = "c4-cell" + (board[r][c] === 1 ? " p1" : board[r][c] === 2 ? " p2" : "");
      cell.addEventListener("click", () => sendConnectFourMove(c));
      rowEl.appendChild(cell);
    }
    c4BoardEl.appendChild(rowEl);
  }
}

// Greys out empty cells (real discs already have their own cursor: default
// via .p1/.p2) whenever it isn't this client's turn, so idle clicks read
// as visibly inert instead of silently doing nothing.
function updateConnectFourTurnUI() {
  c4BoardEl.classList.toggle("not-your-turn", c4CurrentTurn !== playerId);
}

function sendConnectFourMove(column) {
  if (!c4SessionId || c4CurrentTurn !== playerId) return;
  send("CONNECT_FOUR_MOVE", { session_id: c4SessionId, column });
}

function onConnectFourWaiting(payload) {
  wordGameSessionId = null;
  wordgameEl.style.display = "none";
  connectionsSessionId = null;
  connectionsEl.style.display = "none";

  c4SessionId = payload.session_id;
  c4YourSymbol = null;
  c4CurrentTurn = null;
  buildConnectFourBoard(
    Array.from({ length: 6 }, () => Array(7).fill(0)),
  );
  c4StatusEl.textContent = "Waiting for an opponent to join...";
  showGamePanel(connectFourEl, "Connect Four");
}

function onConnectFourStarted(payload) {
  wordGameSessionId = null;
  wordgameEl.style.display = "none";
  connectionsSessionId = null;
  connectionsEl.style.display = "none";

  c4SessionId = payload.session_id;
  c4YourSymbol = payload.your_symbol;
  c4CurrentTurn = payload.first_turn_player_id;
  buildConnectFourBoard(payload.board);
  updateConnectFourTurnUI();
  const opponentName = payload.opponent_display_name || payload.opponent_id.replace(/^player_/, "");
  const yourColor = c4YourSymbol === 1 ? "red" : "yellow";
  const turn = c4CurrentTurn === playerId ? "your turn" : "their turn";
  c4StatusEl.textContent = `Playing against ${opponentName} — you're ${yourColor}, ${turn}.`;
  showGamePanel(connectFourEl, "Connect Four");
}

function onConnectFourResult(payload) {
  if (payload.session_id !== c4SessionId) return;
  buildConnectFourBoard(payload.board);
  c4CurrentTurn = payload.current_turn_player_id || null;
  updateConnectFourTurnUI();

  if (payload.status === "WON") {
    c4StatusEl.textContent = payload.winner_id === playerId ? "You won! 🎉" : "You lost — better luck next time.";
  } else if (payload.status === "DRAW") {
    c4StatusEl.textContent = "It's a draw!";
  } else {
    c4StatusEl.textContent = c4CurrentTurn === playerId ? "Your turn" : "Their turn";
  }
}

function onConnectFourOpponentLeft(payload) {
  if (payload.session_id !== c4SessionId) return;
  c4CurrentTurn = null;
  updateConnectFourTurnUI();
  c4StatusEl.textContent = "Your opponent disconnected — you win!";
}

const ROOM_WIDTH = GRID_COLS * GRID_SIZE;
const ROOM_HEIGHT = GRID_ROWS * GRID_SIZE;
const WALL_HEIGHT = 3 * GRID_SIZE;
// Rows above this are the wallpapered wall — not floor, so not walkable.
const FLOOR_TOP_ROW = WALL_HEIGHT / GRID_SIZE;

// "The Parlor" — code-drawn (no external art) so it stays in sync with
// GAME_TABLES without needing a separate image asset to keep in step.
// Static decor (wall, rug, fireplace) draws once; the 3 GAME_TABLES spots
// get their own glow ring + bobbing badge + hover highlight below.
function drawRoomDecor(scene) {
  const g = scene.add.graphics();

  g.fillStyle(0x9c6a42, 1);
  g.fillRect(0, 0, ROOM_WIDTH, ROOM_HEIGHT);
  for (let x = 42; x < ROOM_WIDTH; x += 44) {
    g.fillStyle(0x8a5a36, 1);
    g.fillRect(x, WALL_HEIGHT, 2, ROOM_HEIGHT - WALL_HEIGHT);
  }

  g.fillStyle(0xe9d6c2, 1);
  g.fillRect(0, 0, ROOM_WIDTH, WALL_HEIGHT - 16);
  g.fillStyle(0x9c5a52, 0.14);
  for (let x = 4; x < ROOM_WIDTH; x += 16) {
    for (let y = 4; y < WALL_HEIGHT - 22; y += 16) {
      g.fillRect(x, y, 2, 2);
    }
  }
  g.fillStyle(0x6a4530, 1);
  g.fillRect(0, WALL_HEIGHT - 16, ROOM_WIDTH, 12);
  g.fillStyle(0x4a3020, 1);
  g.fillRect(0, WALL_HEIGHT - 4, ROOM_WIDTH, 4);

  drawSconce(scene, 40, 34);
  drawSconce(scene, ROOM_WIDTH - 40, 34);
  drawFrame(scene, 110, 28, 0x7fa8e0);
  drawFrame(scene, ROOM_WIDTH - 110, 28, 0x6fbf6f);
  drawFireplace(scene, ROOM_WIDTH / 2, WALL_HEIGHT - 16);

  const rugW = 232;
  const rugH = 172;
  const rugX = ROOM_WIDTH / 2 - rugW / 2;
  const rugY = WALL_HEIGHT + 14;
  g.fillStyle(0xe8c9a0, 1);
  g.fillRoundedRect(rugX, rugY, rugW, rugH, 8);
  g.fillStyle(0xc96a5a, 1);
  g.fillRoundedRect(rugX + 7, rugY + 7, rugW - 14, rugH - 14, 6);
  g.lineStyle(3, 0xe8c9a0, 0.55);
  g.strokeRoundedRect(rugX + 20, rugY + 20, rugW - 40, rugH - 40, 4);
}

function drawSconce(scene, x, y) {
  const g = scene.add.graphics();
  g.fillStyle(0xffe9b0, 0.35);
  g.fillCircle(x, y - 9, 8);
  g.fillStyle(0xd8c2a0, 1);
  g.fillRoundedRect(x - 4, y, 8, 16, 2);
  g.lineStyle(1.5, 0x4a3426, 1);
  g.strokeRoundedRect(x - 4, y, 8, 16, 2);
}

function drawFrame(scene, x, y, artColor) {
  const g = scene.add.graphics();
  g.fillStyle(0xb58a5c, 1);
  g.fillRect(x - 15, y, 30, 40);
  g.lineStyle(3, 0xcaa15c, 1);
  g.strokeRect(x - 15, y, 30, 40);
  g.fillStyle(artColor, 1);
  g.fillRect(x - 9, y + 6, 18, 28);
}

function drawFireplace(scene, x, bottomY) {
  const width = 80;
  const height = 54;
  const g = scene.add.graphics();
  g.fillStyle(0x7a5638, 1);
  g.fillRoundedRect(x - width / 2 - 6, bottomY - height - 12, width + 12, 12, 2);
  g.fillStyle(0xd8e4e8, 1);
  g.fillRoundedRect(x - 25, bottomY - height - 34, 50, 24, 12);
  g.lineStyle(3, 0xcaa15c, 1);
  g.strokeRoundedRect(x - 25, bottomY - height - 34, 50, 24, 12);
  g.fillStyle(0x8a8580, 1);
  g.fillRect(x - width / 2, bottomY - height, width, height);
  g.lineStyle(3, 0x4a3426, 1);
  g.strokeRect(x - width / 2, bottomY - height, width, height);
  g.fillStyle(0x2a1c14, 1);
  g.fillRoundedRect(x - 24, bottomY - 44, 48, 44, [4, 4, 2, 2]);

  const glow = scene.add.graphics();
  glow.fillStyle(0xe8a33d, 0.16);
  glow.fillCircle(x, bottomY - 20, 60);

  const flame = scene.add.container(x, bottomY - 12);
  const flameG = scene.add.graphics();
  flameG.fillStyle(0xd9524f, 1);
  flameG.fillEllipse(0, 0, 16, 24);
  flameG.fillStyle(0xe8a33d, 1);
  flameG.fillEllipse(0, 2, 11, 17);
  flameG.fillStyle(0xffce6a, 1);
  flameG.fillEllipse(0, 4, 6, 10);
  flame.add(flameG);
  scene.tweens.add({
    targets: flame,
    scaleX: 0.88,
    scaleY: 1.1,
    duration: 700,
    yoyo: true,
    repeat: -1,
    ease: "Sine.easeInOut",
  });
}

// Small icon drawn inside a spot's floating badge, one per game type —
// lets the badge communicate *what* you'll get, not just that it's
// clickable.
function addBadgeIcon(scene, container, gameType) {
  if (gameType === "word") {
    const label = scene.add.text(0, 0, "W", {
      fontFamily: "Fredoka, sans-serif",
      fontSize: "14px",
      fontStyle: "700",
      color: "#2a2a2a",
    });
    label.setOrigin(0.5);
    container.add(label);
    return;
  }
  if (gameType === "connect_four") {
    const g = scene.add.graphics();
    g.fillStyle(0xd9524f, 1);
    g.fillCircle(-4, 1, 5.5);
    g.fillStyle(0xe8c93d, 1);
    g.fillCircle(4, 1, 5.5);
    container.add(g);
    return;
  }
  const g = scene.add.graphics();
  const colors = [0xe8d84a, 0x6fbf6f, 0x7fa8e0, 0xb98fd6];
  const offsets = [
    [-4, -4],
    [3, -4],
    [-4, 3],
    [3, 3],
  ];
  colors.forEach((color, i) => {
    g.fillStyle(color, 1);
    g.fillRect(offsets[i][0], offsets[i][1], 6, 6);
  });
  container.add(g);
}

// Draws the furniture for one GAME_TABLES entry, centered on its cell —
// deriving position straight from the same data findTableAt() uses, so
// the art can never drift out of sync with what's actually walkable.
// Draws relative to (0,0) — the caller wraps the result in a container
// positioned at the table's cell center, so the whole piece scales
// around its own middle on hover instead of the world origin.
function drawTableFurniture(scene, table) {
  const g = scene.add.graphics();
  const pieces = [g];

  if (table.gameType === "word") {
    g.fillStyle(0xa5693e, 1);
    g.fillRoundedRect(-11, 18, 22, 20, 3);
    g.lineStyle(2, 0x4a3426, 1);
    g.strokeRoundedRect(-11, 18, 22, 20, 3);
    g.fillStyle(0xe8c93d, 1);
    g.fillRoundedRect(26, -26, 12, 18, [2, 2, 6, 6]);
    g.lineStyle(1.5, 0x4a3426, 1);
    g.strokeRoundedRect(26, -26, 12, 18, [2, 2, 6, 6]);
    g.fillStyle(0x7a5638, 1);
    g.fillRoundedRect(-48, -22, 96, 44, 4);
    g.lineStyle(2, 0x4a3426, 1);
    g.strokeRoundedRect(-48, -22, 96, 44, 4);
    ["C", "A", "T"].forEach((letter, i) => {
      const tx = -21 + i * 19;
      g.fillStyle(0xfffaf3, 1);
      g.fillRoundedRect(tx, -7, 14, 14, 2);
      g.lineStyle(1.5, 0x2a2a2a, 1);
      g.strokeRoundedRect(tx, -7, 14, 14, 2);
      const label = scene.add
        .text(tx + 7, 0, letter, { fontFamily: "Fredoka, sans-serif", fontSize: "9px", fontStyle: "700", color: "#2a2a2a" })
        .setOrigin(0.5);
      pieces.push(label);
    });
  } else if (table.gameType === "connect_four") {
    ["left", "right"].forEach((side) => {
      const cx = side === "left" ? -70 : 70;
      g.fillStyle(0xa5693e, 1);
      g.fillRoundedRect(cx - 10, -6, 20, 26, 4);
      g.lineStyle(2, 0x4a3426, 1);
      g.strokeRoundedRect(cx - 10, -6, 20, 26, 4);
    });
    g.fillStyle(0x7a5638, 1);
    g.fillCircle(0, 0, 54);
    g.lineStyle(3, 0x4a3426, 1);
    g.strokeCircle(0, 0, 54);
    g.fillStyle(0x2b5aa8, 1);
    g.fillCircle(0, 0, 41);
    const chipColors = [0x14141c, 0xd9524f, 0xe8c93d];
    for (let row = -1; row <= 1; row++) {
      for (let col = -1; col <= 1; col++) {
        g.fillStyle(chipColors[Math.abs(row * 3 + col) % chipColors.length], 1);
        g.fillCircle(col * 12, row * 12, 5.5);
      }
    }
  } else {
    g.fillStyle(0xa5693e, 1);
    g.fillCircle(0, 0, 42);
    g.lineStyle(3, 0x4a3426, 1);
    g.strokeCircle(0, 0, 42);
    const cardColors = [0xe8d84a, 0x6fbf6f, 0x7fa8e0, 0xb98fd6];
    const cardAngles = [-18, -6, 6, 18];
    cardColors.forEach((color, i) => {
      const card = scene.add.graphics();
      card.fillStyle(color, 1);
      card.fillRoundedRect(-9, -14, 18, 24, 3);
      card.lineStyle(2, 0x4a3426, 1);
      card.strokeRoundedRect(-9, -14, 18, 24, 3);
      const cardContainer = scene.add.container((i - 1.5) * 11, 0, [card]);
      cardContainer.setAngle(cardAngles[i]);
      pieces.push(cardContainer);
    });
  }

  return pieces;
}

// A tween that pulses/bobs forever — captured behind a start function so
// hover can kill it and restart it later, instead of layering a second
// tween on the same properties (which fights the idle one and reads as
// a flash instead of a held highlight — the same bug already fixed once
// this session for player movement, in onPlayerMoved).
function startIdlePulse(scene, target, props) {
  return scene.tweens.add({ targets: target, ...props, yoyo: true, repeat: -1, ease: "Sine.easeInOut" });
}

function createInteractiveSpot(scene, table) {
  const center = cellCenter(table.cell.x, table.cell.y);
  const furniturePieces = drawTableFurniture(scene, table);
  const furniture = scene.add.container(center.x, center.y, furniturePieces);

  // Always-visible pulsing "walk here" glow — not hover-gated, so it
  // reads as interactive on touch devices too.
  const glow = scene.add.graphics();
  glow.fillStyle(0xe8a33d, 0.32);
  glow.fillEllipse(0, 0, 92, 30);
  glow.fillStyle(0xe8a33d, 0.5);
  glow.fillEllipse(0, 0, 56, 18);
  const glowContainer = scene.add.container(center.x, center.y + 32, [glow]);
  let glowTween = startIdlePulse(scene, glowContainer, { scaleX: 1.12, scaleY: 1.12, alpha: { from: 0.75, to: 1 }, duration: 1300 });

  // Floating badge — bobs continuously (works without hover); brightens
  // further on hover as a desktop-only bonus, layered on top of that.
  const badgeY = center.y - 46;
  const badgeBg = scene.add.graphics();
  badgeBg.fillStyle(0xfffaf3, 1);
  badgeBg.fillRoundedRect(-15, -15, 30, 30, 9);
  badgeBg.lineStyle(2, 0x4a3426, 1);
  badgeBg.strokeRoundedRect(-15, -15, 30, 30, 9);
  const badge = scene.add.container(center.x, badgeY, [badgeBg]);
  addBadgeIcon(scene, badge, table.gameType);
  let badgeTween = startIdlePulse(scene, badge, { y: badgeY - 6, duration: 1100 });

  const hoverZone = scene.add.zone(center.x, center.y, 130, 130).setOrigin(0.5).setInteractive();
  hoverZone.on("pointerover", () => {
    scene.tweens.killTweensOf([glowContainer, badge, furniture]);
    scene.tweens.add({ targets: glowContainer, scaleX: 1.3, scaleY: 1.3, alpha: 1, duration: 120 });
    scene.tweens.add({ targets: badge, y: badgeY, scaleX: 1.15, scaleY: 1.15, duration: 120 });
    scene.tweens.add({ targets: furniture, scaleX: 1.08, scaleY: 1.08, duration: 120 });
  });
  hoverZone.on("pointerout", () => {
    scene.tweens.killTweensOf([glowContainer, badge, furniture]);
    scene.tweens.add({ targets: furniture, scaleX: 1, scaleY: 1, duration: 150 });
    scene.tweens.add({
      targets: glowContainer,
      scaleX: 1,
      scaleY: 1,
      alpha: 0.75,
      duration: 150,
      onComplete: () => {
        glowTween = startIdlePulse(scene, glowContainer, { scaleX: 1.12, scaleY: 1.12, alpha: { from: 0.75, to: 1 }, duration: 1300 });
      },
    });
    scene.tweens.add({
      targets: badge,
      y: badgeY,
      scaleX: 1,
      scaleY: 1,
      duration: 150,
      onComplete: () => {
        badgeTween = startIdlePulse(scene, badge, { y: badgeY - 6, duration: 1100 });
      },
    });
  });
}

function drawRoom(scene) {
  drawRoomDecor(scene);
  for (const table of GAME_TABLES) {
    createInteractiveSpot(scene, table);
  }
}

document.getElementById("create-room").addEventListener("click", () => send("CREATE_ROOM", {}));
document.getElementById("join-room").addEventListener("click", () => {
  const code = roomCodeInput.value.trim().toUpperCase();
  if (!code) return;
  send("JOIN_ROOM", {
    player_id: playerId,
    room_code: code,
    display_name: currentDisplayName(),
    critter_type: currentCritter(),
  });
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
      preloadCritterTextures(this);
    },
    create: function () {
      scene = this;
      registerCritterAnimations(this);
      drawRoom(this);
      this.input.on("pointerdown", (pointer) => {
        if (!roomCode) return;
        // pointer.x/y go through Phaser's scale-manager transform, which
        // can come back Infinity/NaN in some hosting contexts before it
        // settles — so this maps the click itself, via the native DOM
        // event and the canvas's own bounding box, instead. The canvas's
        // drawing buffer is always GRID_COLS*GRID_SIZE x GRID_ROWS*
        // GRID_SIZE (512x288) regardless of ZOOM — zoom is a display-size
        // hint that index.html's `#game canvas { width: 100% }` already
        // overrides so the canvas fits any container width (mobile
        // included) — so mapping a click back to buffer pixels via
        // getBoundingClientRect lands directly in the same coordinate
        // space cellCenter()/GRID_SIZE already use, no ZOOM involved.
        const nativeEvent = pointer.event;
        if (!nativeEvent) return;
        // On touch devices the native event can be a plain TouchEvent
        // rather than a PointerEvent/MouseEvent — TouchEvent has no
        // clientX/clientY of its own, only per-finger entries in
        // .touches/.changedTouches, so pull from there when needed.
        const point = "clientX" in nativeEvent ? nativeEvent : (nativeEvent.touches?.[0] ?? nativeEvent.changedTouches?.[0]);
        if (!point) return;
        const canvasEl = this.sys.game.canvas;
        const rect = canvasEl.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return;
        const scaleX = canvasEl.width / rect.width;
        const scaleY = canvasEl.height / rect.height;
        const offsetX = (point.clientX - rect.left) * scaleX;
        const offsetY = (point.clientY - rect.top) * scaleY;
        const targetX = Math.floor(offsetX / GRID_SIZE);
        const rawTargetY = Math.floor(offsetY / GRID_SIZE);
        if (!Number.isFinite(targetX) || !Number.isFinite(rawTargetY)) return;
        // The wall (top rows) isn't floor — clamp taps there to the
        // nearest walkable row instead of letting cats stand on it.
        const targetY = Math.max(rawTargetY, FLOOR_TOP_ROW);
        const table = findTableAt(targetX, targetY);
        pendingGameTable = table;
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

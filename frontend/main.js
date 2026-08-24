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
const lobbyEl = document.getElementById("lobby");
const gameEl = document.getElementById("game");

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
      lobbyEl.style.display = "none";
      gameEl.style.display = "block";
      break;
    case "PLAYER_MOVED":
      onPlayerMoved(env.payload);
      break;
    case "ERROR":
      setStatus(`error: ${env.payload.message}`);
      break;
  }
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

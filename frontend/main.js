const WS_PORT = 8080;

function create() {
  const statusText = this.add.text(16, 16, "connecting...", {
    fontSize: "16px",
    color: "#ffffff",
  });

  const socket = new WebSocket(`ws://${location.hostname}:${WS_PORT}/ws`);

  socket.addEventListener("open", () => statusText.setText("connected"));
  socket.addEventListener("close", () => statusText.setText("disconnected"));
  socket.addEventListener("error", () => statusText.setText("connection error"));
  socket.addEventListener("message", (event) => {
    statusText.setText(`received: ${event.data}`);
  });

  document.getElementById("send-test").addEventListener("click", () => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(`hello from frontend @ ${new Date().toISOString()}`);
    }
  });
}

new Phaser.Game({
  type: Phaser.AUTO,
  width: 480,
  height: 270,
  parent: "game",
  backgroundColor: "#2d2d3a",
  scene: { create },
});

const WebSocket = require("ws");

const wss = new WebSocket.Server({ port: process.env.PORT || 8080 });

// code -> { host: WebSocket, peers: Map(peerId -> WebSocket), nextPeerId: int }
const rooms = new Map();
// WebSocket -> { roomCode: string, peerId: int, isHost: bool }
const clients = new Map();

function generateCode() {
	let code;
	do { code = Math.floor(1000 + Math.random() * 9000).toString(); }
	while (rooms.has(code));
	return code;
}

function send(ws, data) {
	if (ws && ws.readyState === WebSocket.OPEN)
		ws.send(JSON.stringify(data));
}

wss.on("connection", (ws) => {
	ws.on("message", (raw) => {
		let msg;
		try { msg = JSON.parse(raw); } catch { return; }

		switch (msg.type) {
			case "create": {
				const code = generateCode();
				rooms.set(code, { host: ws, peers: new Map(), nextPeerId: 2 });
				clients.set(ws, { roomCode: code, peerId: 1, isHost: true });
				send(ws, { type: "created", code });
				break;
			}
			case "join": {
				const room = rooms.get(msg.code);
				if (!room) {
					send(ws, { type: "error", message: "Room not found" });
					return;
				}
				const peerId = room.nextPeerId++;
				room.peers.set(peerId, ws);
				clients.set(ws, { roomCode: msg.code, peerId, isHost: false });
				send(ws, { type: "joined", peerId });
				send(room.host, { type: "peer_joined", peerId });
				break;
			}
			case "offer":
			case "answer": {
				const info = clients.get(ws);
				if (!info) return;
				const room = rooms.get(info.roomCode);
				if (!room) return;
				if (info.isHost) {
					send(room.peers.get(msg.peerId), { type: msg.type, sdp: msg.sdp, peerId: 1 });
				} else {
					send(room.host, { type: msg.type, sdp: msg.sdp, peerId: info.peerId });
				}
				break;
			}
			case "candidate": {
				const info = clients.get(ws);
				if (!info) return;
				const room = rooms.get(info.roomCode);
				if (!room) return;
				const fwd = { type: "candidate", mid: msg.mid, index: msg.index, sdp: msg.sdp };
				if (info.isHost) {
					send(room.peers.get(msg.peerId), { ...fwd, peerId: 1 });
				} else {
					send(room.host, { ...fwd, peerId: info.peerId });
				}
				break;
			}
		}
	});

	ws.on("close", () => {
		const info = clients.get(ws);
		if (!info) return;
		clients.delete(ws);
		const room = rooms.get(info.roomCode);
		if (!room) return;
		if (info.isHost) {
			room.peers.forEach((peerWs) => send(peerWs, { type: "host_disconnected" }));
			rooms.delete(info.roomCode);
		} else {
			room.peers.delete(info.peerId);
			send(room.host, { type: "peer_left", peerId: info.peerId });
		}
	});
});

console.log("Signaling server running on port", process.env.PORT || 8080);

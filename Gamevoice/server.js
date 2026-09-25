const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;

const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map(); // roomId -> Set of clients

wss.on('connection', (ws) => {
  let roomId = null;
  let clientId = Math.random().toString(36).slice(2, 11);

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'join') {
        roomId = data.room;
        if (!rooms.has(roomId)) rooms.set(roomId, new Set());
        rooms.get(roomId).add(ws);
        ws.clientId = clientId;
        ws.roomId = roomId;
        ws.name = data.name;

        // Отправляем новичку список уже подключённых
        const peers = [];
        for (const client of rooms.get(roomId)) {
          if (client !== ws && client.clientId) {
            peers.push({ id: client.clientId, name: client.name });
          }
        }
        ws.send(JSON.stringify({ type: 'peers', peers }));

        // Уведомляем остальных
        broadcast(roomId, {
          type: 'peer-joined',
          id: clientId,
          name: data.name
        }, ws);
      }

      else if (data.type === 'signal') {
        const target = findClient(roomId, data.target);
        if (target) {
          target.send(JSON.stringify({
            type: 'signal',
            from: clientId,
            data: data.data
          }));
        }
      }

      else if (data.type === 'chat') {
        broadcast(roomId, {
          type: 'chat',
          from: clientId,
          name: ws.name,
          text: data.text,
          ts: data.ts
        }, ws);
      }

      else if (data.type === 'mute') {
        broadcast(roomId, {
          type: 'mute',
          id: clientId,
          muted: data.muted
        }, ws);
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    if (roomId && rooms.has(roomId)) {
      rooms.get(roomId).delete(ws);
      if (rooms.get(roomId).size === 0) rooms.delete(roomId);
      broadcast(roomId, { type: 'peer-left', id: clientId }, ws);
    }
  });
});

function broadcast(roomId, message, except) {
  if (!rooms.has(roomId)) return;
  for (const client of rooms.get(roomId)) {
    if (client !== except && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  }
}

function findClient(roomId, id) {
  if (!rooms.has(roomId)) return null;
  for (const client of rooms.get(roomId)) {
    if (client.clientId === id) return client;
  }
  return null;
}

console.log(`Signal server running on port ${PORT}`);
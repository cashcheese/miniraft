const express = require('express');
const http = require('http');
const ws = require('ws');
const axios = require('axios');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new ws.Server({ noServer: true });

const PORT = process.env.PORT || 3000;
const REPLICA_URLS = (process.env.REPLICA_URLS || '').split(',');
const GATEWAY_URL = process.env.GATEWAY_URL || 'http://gateway:3000';

let clients = new Set();
let currentLeaderUrl = null;
let committedStrokes = [];  // Store history of committed strokes

app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'frontend')));

// Enable CORS for local development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// Find leader by polling all replicas
async function findLeader() {
  for (const url of REPLICA_URLS) {
    try {
      const response = await axios.get(`${url}/status`, { timeout: 1000 });
      if (response.data && response.data.state === 'leader') {
        console.log(`[GATEWAY] Found leader at ${url}`);
        currentLeaderUrl = url;
        return url;
      }
    } catch (err) {
      // Replica not available yet
    }
  }
  return null;
}

// Start leader discovery loop
async function startLeaderDiscovery() {
  setInterval(async () => {
    if (!currentLeaderUrl) {
      const leader = await findLeader();
      if (!leader) {
        console.log('[GATEWAY] No leader available');
      }
    }
  }, 1000);
}

// POST /stroke - forward to leader
app.post('/stroke', async (req, res) => {
  const { stroke } = req.body;
  
  if (!stroke) {
    return res.status(400).json({ error: 'Missing stroke' });
  }

  if (!currentLeaderUrl) {
    const leader = await findLeader();
    if (!leader) {
      return res.status(503).json({ error: 'no leader available' });
    }
  }

  try {
    const response = await axios.post(`${currentLeaderUrl}/stroke`, { stroke }, { timeout: 2000 });
    return res.status(200).json(response.data);
  } catch (err) {
    // Leader failed, try to find new one
    currentLeaderUrl = null;
    const newLeader = await findLeader();
    
    if (!newLeader) {
      return res.status(503).json({ error: 'no leader available' });
    }

    try {
      const retryResponse = await axios.post(`${newLeader}/stroke`, { stroke }, { timeout: 2000 });
      return res.status(200).json(retryResponse.data);
    } catch (retryErr) {
      return res.status(503).json({ error: 'leader unavailable' });
    }
  }
});

// POST /committed - broadcast to all clients
app.post('/committed', (req, res) => {
  const { stroke, index } = req.body;
  
  // Store in history for new clients
  committedStrokes.push({ stroke, index });
  
  const message = JSON.stringify({ type: 'stroke', stroke, index });
  let broadcastCount = 0;
  
  clients.forEach((client) => {
    if (client.readyState === ws.OPEN) {
      client.send(message);
      broadcastCount++;
    }
  });

  console.log(`[GATEWAY] Broadcast to ${broadcastCount} clients: stroke index ${index}`);
  res.status(200).json({ ok: true, broadcastCount });
});

// POST /clear - broadcast clear command to all clients
app.post('/clear', (req, res) => {
  const message = JSON.stringify({ type: 'clear' });
  let broadcastCount = 0;
  
  clients.forEach((client) => {
    if (client.readyState === ws.OPEN) {
      client.send(message);
      broadcastCount++;
    }
  });

  console.log(`[GATEWAY] Broadcast clear command to ${broadcastCount} clients`);
  
  // Also clear stroke history
  committedStrokes = [];
  
  res.status(200).json({ ok: true, broadcastCount });
});

// GET /status - gateway status
app.get('/status', (req, res) => {
  res.status(200).json({
    leaderUrl: currentLeaderUrl,
    clientCount: clients.size
  });
});

// WebSocket upgrade
server.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

// WebSocket connection handler
wss.on('connection', (wsClient) => {
  clients.add(wsClient);
  console.log(`[GATEWAY] Client connected. Total clients: ${clients.size}`);
  
  // Send all previously committed strokes to new client
  committedStrokes.forEach((item) => {
    const message = JSON.stringify({ type: 'stroke', stroke: item.stroke, index: item.index });
    wsClient.send(message);
  });
  console.log(`[GATEWAY] Synced ${committedStrokes.length} strokes to new client`);

  wsClient.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'stroke' && msg.stroke) {
        // Forward stroke to POST /stroke endpoint
        axios.post('http://localhost:3000/stroke', { stroke: msg.stroke }).catch((err) => {
          console.error('[GATEWAY] Failed to forward stroke:', err.message);
        });
      }
    } catch (err) {
      console.error('[GATEWAY] Failed to parse WebSocket message:', err);
    }
  });

  wsClient.on('close', () => {
    clients.delete(wsClient);
    console.log(`[GATEWAY] Client disconnected. Total clients: ${clients.size}`);
  });

  wsClient.on('error', (err) => {
    console.error('[GATEWAY] WebSocket error:', err.message);
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`[GATEWAY] Server listening on port ${PORT}`);
  startLeaderDiscovery();
});

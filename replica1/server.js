const express = require('express');
const axios = require('axios');

const app = express();

const PORT = process.env.PORT || 4001;
const REPLICA_ID = process.env.REPLICA_ID || 'replica1';
const PEERS = (process.env.PEERS || '').split(',').filter(p => p.trim());
const GATEWAY_URL = process.env.GATEWAY_URL || 'http://gateway:3000';

app.use(express.json());

// RAFT Constants
const ELECTION_TIMEOUT_MIN = 500;
const ELECTION_TIMEOUT_MAX = 800;
const HEARTBEAT_INTERVAL = 150;

// RAFT State
let state = 'follower'; // follower, candidate, leader
let currentTerm = 0;
let votedFor = null;
let log = []; // array of { index, term, entry }
let commitIndex = -1;
let leaderId = null;
let votes = 0;

let electionTimer = null;
let heartbeatTimer = null;
let nextIndex = {}; // for leader: track next index for each peer
let matchIndex = {}; // for leader: track match index for each peer

// Initialize nextIndex and matchIndex
function initLeaderState() {
  for (const peer of PEERS) {
    nextIndex[peer] = log.length;
    matchIndex[peer] = -1;
  }
}

// Reset election timer with random timeout
function resetElectionTimer() {
  if (electionTimer) clearTimeout(electionTimer);
  const timeout = ELECTION_TIMEOUT_MIN + Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
  electionTimer = setTimeout(() => {
    startElection();
  }, timeout);
}

// Stop all timers
function clearAllTimers() {
  if (electionTimer) clearTimeout(electionTimer);
  if (heartbeatTimer) clearInterval(heartbeatTimer);
}

// Step down to follower
function stepDown(term) {
  if (term > currentTerm) {
    console.log(`[${REPLICA_ID}] Step down: term ${currentTerm} -> ${term}`);
    currentTerm = term;
    state = 'follower';
    votedFor = null;
    leaderId = null;
    clearAllTimers();
    resetElectionTimer();
  }
}

// Start election
function startElection() {
  if (state === 'leader') return;

  currentTerm += 1;
  state = 'candidate';
  votedFor = REPLICA_ID;
  votes = 1; // vote for self
  leaderId = null;

  console.log(`[${REPLICA_ID}] Became CANDIDATE at term ${currentTerm}`);

  resetElectionTimer();

  // Request votes from all peers
  PEERS.forEach((peerUrl) => {
    axios.post(`${peerUrl}/request-vote`, {
      term: currentTerm,
      candidateId: REPLICA_ID
    }, { timeout: 1000 })
      .then((response) => {
        if (response.data.voteGranted && state === 'candidate' && response.data.term === currentTerm) {
          votes++;
          console.log(`[${REPLICA_ID}] Received vote from peer, votes: ${votes}`);

          // Check if we have a quorum (3 replicas: need 2 votes total including self)
          if (votes >= 2 && state === 'candidate') {
            becomeLeader();
          }
        }
        // If response has higher term, step down
        if (response.data.term > currentTerm) {
          stepDown(response.data.term);
        }
      })
      .catch(() => {
        // Peer not available
      });
  });
}

// Become leader
function becomeLeader() {
  state = 'leader';
  leaderId = REPLICA_ID;
  clearAllTimers();

  console.log(`[${REPLICA_ID}] Became LEADER at term ${currentTerm}`);

  initLeaderState();

  // Send heartbeats immediately, then every HEARTBEAT_INTERVAL
  sendHeartbeats();
  heartbeatTimer = setInterval(() => {
    sendHeartbeats();
  }, HEARTBEAT_INTERVAL);
}

// Send heartbeats to all peers
function sendHeartbeats() {
  if (state !== 'leader') return;

  PEERS.forEach((peerUrl) => {
    axios.post(`${peerUrl}/heartbeat`, {
      term: currentTerm,
      leaderId: REPLICA_ID,
      commitIndex: commitIndex
    }, { timeout: 1000 })
      .then((response) => {
        if (response.data.term > currentTerm) {
          stepDown(response.data.term);
        }
      })
      .catch(() => {
        // Peer not available
      });
  });
}

// Sync peer with log entries
async function syncPeer(peerUrl, fromIndex) {
  try {
    const entries = log.slice(fromIndex);
    const prevLogIndex = fromIndex - 1;
    const prevLogTerm = log[prevLogIndex] ? log[prevLogIndex].term : -1;

    const response = await axios.post(`${peerUrl}/append-entries`, {
      term: currentTerm,
      leaderId: REPLICA_ID,
      prevLogIndex: prevLogIndex,
      prevLogTerm: prevLogTerm,
      entries: entries,
      commitIndex: commitIndex
    }, { timeout: 2000 });

    if (response.data.success) {
      matchIndex[peerUrl] = log.length - 1;
      nextIndex[peerUrl] = log.length;
      
      // Check if we can advance commit index
      if (state === 'leader') {
        updateCommitIndex();
      }
    } else {
      // Failed, decrement nextIndex
      const newNextIndex = Math.max(0, nextIndex[peerUrl] - 1);
      nextIndex[peerUrl] = newNextIndex;
    }

    if (response.data.term > currentTerm) {
      stepDown(response.data.term);
    }
  } catch (err) {
    // Sync failed
  }
}

// Update commit index (leader only)
function updateCommitIndex() {
  if (state !== 'leader') return;

  for (let n = log.length - 1; n > commitIndex; n--) {
    let replicationCount = 1; // count self

    for (const peerUrl of PEERS) {
      if (matchIndex[peerUrl] >= n) {
        replicationCount++;
      }
    }

    // If replicated to majority, advance commitIndex
    if (replicationCount >= 2) {
      commitIndex = n;
      console.log(`[${REPLICA_ID}] Advanced commitIndex to ${commitIndex}`);
      break;
    }
  }
}

// HTTP Endpoints

// POST /request-vote
app.post('/request-vote', (req, res) => {
  const { term, candidateId } = req.body;

  if (term < currentTerm) {
    return res.status(200).json({ term: currentTerm, voteGranted: false });
  }

  if (term > currentTerm) {
    stepDown(term);
  }

  if (votedFor === null || votedFor === candidateId) {
    votedFor = candidateId;
    resetElectionTimer();
    console.log(`[${REPLICA_ID}] Voted for ${candidateId} in term ${currentTerm}`);
    return res.status(200).json({ term: currentTerm, voteGranted: true });
  }

  return res.status(200).json({ term: currentTerm, voteGranted: false });
});

// POST /append-entries
app.post('/append-entries', (req, res) => {
  const { term, leaderId: requestLeaderId, prevLogIndex, prevLogTerm, entries, commitIndex: leaderCommitIndex } = req.body;

  if (term < currentTerm) {
    return res.status(200).json({ term: currentTerm, success: false, logLength: log.length });
  }

  // Step down if needed
  if (term >= currentTerm) {
    if (term > currentTerm) {
      currentTerm = term;
    }
    if (state === 'candidate' || state === 'leader') {
      state = 'follower';
      clearAllTimers();
    }
    leaderId = requestLeaderId;
    resetElectionTimer();
    console.log(`[${REPLICA_ID}] Accepted appendEntries from ${requestLeaderId} at term ${currentTerm}`);
  }

  // Check log consistency
  if (prevLogIndex >= 0) {
    if (log[prevLogIndex] === undefined || log[prevLogIndex].term !== prevLogTerm) {
      return res.status(200).json({ term: currentTerm, success: false, logLength: log.length });
    }
  }

  // Truncate log and append entries
  if (prevLogIndex + 1 < log.length && entries && entries.length > 0) {
    log = log.slice(0, prevLogIndex + 1);
  }

  if (entries && entries.length > 0) {
    log = log.concat(entries);
  }

  // Update commitIndex
  if (leaderCommitIndex > commitIndex) {
    commitIndex = Math.min(leaderCommitIndex, log.length - 1);
  }

  return res.status(200).json({ term: currentTerm, success: true, logLength: log.length });
});

// POST /heartbeat
app.post('/heartbeat', (req, res) => {
  const { term, leaderId: reqLeaderId, commitIndex: leaderCommitIndex } = req.body;

  if (term < currentTerm) {
    return res.status(200).json({ term: currentTerm, success: false });
  }

  // Step down if needed
  if (term > currentTerm) {
    stepDown(term);
  }

  if (state !== 'follower') {
    state = 'follower';
    clearAllTimers();
  }

  leaderId = reqLeaderId;
  resetElectionTimer();

  // Update commitIndex
  if (leaderCommitIndex > commitIndex) {
    commitIndex = leaderCommitIndex;
  }

  return res.status(200).json({ term: currentTerm, success: true });
});

// GET /sync-log?from=N
app.get('/sync-log', (req, res) => {
  const from = parseInt(req.query.from) || 0;
  const entries = log.slice(from);
  return res.status(200).json({ entries });
});

// POST /stroke
app.post('/stroke', async (req, res) => {
  const { stroke } = req.body;

  if (state !== 'leader') {
    return res.status(403).json({ error: 'not leader', leaderId });
  }

  if (!stroke) {
    return res.status(400).json({ error: 'Missing stroke' });
  }

  // Append to log
  const entry = {
    index: log.length,
    term: currentTerm,
    entry: stroke
  };

  log.push(entry);
  console.log(`[${REPLICA_ID}] Added entry to log at index ${entry.index}`);

  // Send to all peers
  let acks = 1; // count self
  let responses = [];

  const replicationPromises = PEERS.map((peerUrl) =>
    axios.post(`${peerUrl}/append-entries`, {
      term: currentTerm,
      leaderId: REPLICA_ID,
      prevLogIndex: log.length - 2,
      prevLogTerm: log[log.length - 2] ? log[log.length - 2].term : -1,
      entries: [entry],
      commitIndex: commitIndex
    }, { timeout: 2000 })
      .then((response) => {
        if (response.data.success) {
          acks++;
          matchIndex[peerUrl] = entry.index;
          nextIndex[peerUrl] = log.length;
          responses.push({ peer: peerUrl, success: true });
        } else {
          if (response.data.logLength !== undefined) {
            nextIndex[peerUrl] = response.data.logLength;
          }
          responses.push({ peer: peerUrl, success: false });
        }

        if (response.data.term > currentTerm) {
          stepDown(response.data.term);
        }
      })
      .catch((err) => {
        responses.push({ peer: peerUrl, error: err.message });
      })
  );

  await Promise.all(replicationPromises);

  // Check if replicated to majority
  if (acks >= 2) {
    commitIndex = entry.index;
    console.log(`[${REPLICA_ID}] Entry ${entry.index} committed (acks: ${acks})`);

    // Notify gateway
    try {
      await axios.post(`${GATEWAY_URL}/committed`, { stroke, index: entry.index }, { timeout: 2000 });
      console.log(`[${REPLICA_ID}] Notified gateway of committed entry ${entry.index}`);
    } catch (err) {
      console.error(`[${REPLICA_ID}] Failed to notify gateway:`, err.message);
    }

    return res.status(200).json({ success: true, index: entry.index });
  } else {
    return res.status(503).json({ error: 'not enough replicas', acks });
  }
});

// GET /status
app.get('/status', (req, res) => {
  return res.status(200).json({
    replicaId: REPLICA_ID,
    state,
    currentTerm,
    leaderId,
    logLength: log.length,
    commitIndex
  });
});

// Start server
const server = app.listen(PORT, () => {
  console.log(`[${REPLICA_ID}] Server listening on port ${PORT}`);
  resetElectionTimer();
});

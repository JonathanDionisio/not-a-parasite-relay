'use strict';

// Relay server for Not A Parasite.
// It creates rooms, runs the lobby, puts game commands in one order and sends them to everybody.
// It keeps the full list of commands of every game, so a player who comes back can catch up.
// It contains no game rules.

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

// ---------- settings ----------
const PORT = Number(process.env.PORT) || 8080;
const PROTOCOL_VERSION = 3;               // must match NetProtocol.Version in Unity
const MAX_SEATS = 5;
const MIN_SEATS = 2;
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // no I, O, 0 or 1: easy to read out loud
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const EMPTY_ROOM_MS = 5 * 60 * 1000;      // a started room with nobody in it is kept this long
const MAX_ROOM_MS = 12 * 60 * 60 * 1000;
const MAX_LOG = 20000;
const HOST_STALE_MS = 4000;               // the host's game has not reported for this long: hand over
const FRESH_MS = 2500;                    // a player counts as present if they reported this recently
const START_GRACE_MS = 20000; 
const MAX_ROOMS = 100;                    // beyond this the server says it is busy
const MAX_CONNECTIONS_PER_IP = 20;
const connectionsPerIp = new Map();            // time for a game window to load before it is expected to report

const rooms = new Map();                  // code -> room

// ---------- helpers ----------
function makeCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let code = '';
    for (let i = 0; i < 4; i++) code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    if (!rooms.has(code)) return code;
  }
  throw new Error('No free room code');
}

function cleanName(raw) {
  const s = String(raw == null ? '' : raw).replace(/[\u0000-\u001f\u007f<>]/g, '').trim().replace(/\s+/g, ' ');
  return s.slice(0, 10).trim();
}

function clampInt(value, lo, hi, fallback) {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(hi, Math.max(lo, n));
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function sendError(ws, msg) {
  send(ws, { t: 'error', msg });
}

function broadcast(room, obj) {
  for (const p of room.seats) if (p.ws) send(p.ws, obj);
}

function nameTaken(room, name) {
  const lower = name.toLowerCase();
  return room.seats.some(p => p.name.toLowerCase() === lower);
}

function nextBotName(room) {
  for (let n = 1; n <= 99; n++) {
    const name = 'Bot ' + n;
    if (!nameTaken(room, name)) return name;
  }
  return 'Bot';
}

function isHost(ws) {
  return ws.room && ws.room.host === ws.player;
}

// ---------- rooms ----------
function createRoom() {
  const room = {
    code: makeCode(),
    seats: [],                                           // { name, isBot, ws, token, lastAlive }
    host: null,
    settings: { turnSeconds: 60, deckTakes: 3, botSkill: 1 },
    started: false,
    seed: 0,
    startMessage: null,
    log: [],                                             // every command, in order
    createdAt: Date.now(),
    emptySince: null,
  };
  rooms.set(room.code, room);
  console.log(`[${room.code}] room created (${rooms.size} rooms)`);
  return room;
}

function addHuman(room, ws, name) {
  const player = { name, isBot: false, ws, token: crypto.randomBytes(8).toString('hex'), lastAlive: Date.now() };
  room.seats.push(player);
  ws.room = room;
  ws.player = player;
  return player;
}

function seatList(room) {
  return room.seats.map(s => ({ name: s.name, isBot: s.isBot, connected: s.isBot || !!s.ws }));
}

// Each player gets their own copy of the lobby message, because it says which seat they are
function sendLobby(room) {
  const seats = seatList(room);
  const hostSeat = room.seats.indexOf(room.host);
  room.seats.forEach((p, i) => {
    if (p.ws) send(p.ws, { t: 'lobby', code: room.code, started: room.started, hostSeat, you: i, seats, settings: room.settings });
  });
}

// Sends everything a player needs to build or catch up a running game, starting at command number 'from'
function sendResume(ws, room, seat, from) {
  const p = room.seats[seat];
  send(ws, {
    t: 'resume',
    code: room.code,
    token: p ? p.token : '',
    seed: room.seed,
    settings: room.settings,
    seats: seatList(room),
    you: seat,
    hostSeat: room.seats.indexOf(room.host),
    from,
    log: room.log.slice(from).map(e => e.seat + '|' + e.cmd),
  });
}

function pickNewHost(room) {
  room.host = room.seats.find(p => !p.isBot && p.ws) || null;
}

// The real address of a visitor (the hosting service puts it in a header)
function clientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || req.socket.remoteAddress || 'unknown';
}

// Called when somebody leaves or their connection drops
function detach(ws) {
  const room = ws.room;
  const player = ws.player;
  if (!room || !player) return;
  ws.room = null;
  ws.player = null;

  if (!room.started) {
    room.seats.splice(room.seats.indexOf(player), 1);    // in the lobby the seat disappears
  } else {
    player.ws = null;                                    // in a game the seat stays, so the player can come back
  }

  if (room.host === player) pickNewHost(room);

  const humansLeft = room.seats.filter(p => !p.isBot && p.ws).length;
  if (humansLeft === 0) {
    if (!room.started) {
      rooms.delete(room.code);
      console.log(`[${room.code}] room closed (empty)`);
    } else {
      room.emptySince = Date.now();                      // wait a while in case somebody comes back
    }
    return;
  }

  room.emptySince = null;
  sendLobby(room);
}

function closeRoom(room) {
  for (const p of room.seats) {
    if (p.ws) {
      send(p.ws, { t: 'closed' });
      p.ws.room = null;
      p.ws.player = null;
      p.ws.close();
    }
  }
  rooms.delete(room.code);
  console.log(`[${room.code}] room closed (expired)`);
}

// ---------- messages from players ----------
function handle(ws, msg) {
  const room = ws.room;
  const player = ws.player;

  switch (msg.t) {
    case 'host': {
      if (room) return sendError(ws, 'You are already in a room.');
      if (msg.ver !== PROTOCOL_VERSION) return sendError(ws, 'This game version does not match the server. Please reload the page.');
      if (rooms.size >= MAX_ROOMS) return sendError(ws, 'The server is busy. Please try again in a few minutes.');
      const name = cleanName(msg.name);
      if (!name) return sendError(ws, 'Enter a name first.');

      const newRoom = createRoom();
      const p = addHuman(newRoom, ws, name);
      newRoom.host = p;
      send(ws, { t: 'joined', code: newRoom.code, token: p.token });
      sendLobby(newRoom);
      return;
    }

    case 'join': {
      if (room) return sendError(ws, 'You are already in a room.');
      if (msg.ver !== PROTOCOL_VERSION) return sendError(ws, 'This game version does not match the server. Please reload the page.');
      const name = cleanName(msg.name);
      if (!name) return sendError(ws, 'Enter a name first.');

      const code = String(msg.code || '').toUpperCase().trim();
      const target = rooms.get(code);
      if (!target) return sendError(ws, 'There is no room with that code.');
      if (target.started) return sendError(ws, 'That game has already started.');
      if (target.seats.length >= MAX_SEATS) return sendError(ws, 'That room is full.');
      if (nameTaken(target, name)) return sendError(ws, 'Somebody in that room already uses that name. Pick another one.');

      const p = addHuman(target, ws, name);
      send(ws, { t: 'joined', code: target.code, token: p.token });
      sendLobby(target);
      return;
    }

    // Coming back to a running game (after a dropped connection or a reloaded page)
    case 'rejoin': {
      if (room) return sendError(ws, 'You are already in a room.');
      if (msg.ver !== PROTOCOL_VERSION) return sendError(ws, 'This game version does not match the server. Please reload the page.');

      const code = String(msg.code || '').toUpperCase().trim();
      const target = rooms.get(code);
      if (!target || !target.started) return send(ws, { t: 'rejoinfailed', msg: 'That game is no longer running.' });

      const token = String(msg.token || '');
      const seat = target.seats.findIndex(p => !p.isBot && p.token && p.token === token);
      if (seat < 0) return send(ws, { t: 'rejoinfailed', msg: 'Your seat in that game was not found.' });

      const p = target.seats[seat];

      // If the same player is still connected from another window, that window is replaced
      if (p.ws && p.ws !== ws) {
        const old = p.ws;
        old.room = null;
        old.player = null;
        send(old, { t: 'replaced' });
        old.close();
      }

      p.ws = ws;
      p.lastAlive = Date.now() + START_GRACE_MS;
      ws.room = target;
      ws.player = p;
      target.emptySince = null;
      if (!target.host || !target.host.ws) pickNewHost(target);

      sendResume(ws, target, seat, clampInt(msg.from, 0, target.log.length, 0));
      sendLobby(target);
      console.log(`[${target.code}] ${p.name} came back`);
      return;
    }

    // Asks for the commands since number 'from' (a command went missing, or a game was rebuilt)
    case 'resync': {
      if (!room || !room.started || !player) return;
      sendResume(ws, room, room.seats.indexOf(player), clampInt(msg.from, 0, room.log.length, 0));
      return;
    }

    // "My game window is running"
    case 'alive':
      if (player) player.lastAlive = Date.now();
      return;

    case 'leave':
      detach(ws);
      return;

    case 'settings': {
      if (!room || room.started || !isHost(ws)) return;
      const s = msg.settings || {};
      room.settings = {
        turnSeconds: clampInt(s.turnSeconds, 0, 300, room.settings.turnSeconds),
        deckTakes: clampInt(s.deckTakes, 1, 5, room.settings.deckTakes),
        botSkill: clampInt(s.botSkill, 0, 2, room.settings.botSkill),
      };
      sendLobby(room);
      return;
    }

    case 'addbot': {
      if (!room || room.started || !isHost(ws)) return;
      if (room.seats.length >= MAX_SEATS) return sendError(ws, 'The room is full.');
      room.seats.push({ name: nextBotName(room), isBot: true, ws: null, token: null, lastAlive: 0 });
      sendLobby(room);
      return;
    }

    case 'removebot': {
      if (!room || room.started || !isHost(ws)) return;
      const seat = room.seats[msg.seat];
      if (!seat || !seat.isBot) return;
      room.seats.splice(msg.seat, 1);
      sendLobby(room);
      return;
    }

    case 'kick': {
      if (!room || room.started || !isHost(ws)) return;
      const seat = room.seats[msg.seat];
      if (!seat || seat.isBot || seat === player || !seat.ws) return;
      const kickedWs = seat.ws;
      send(kickedWs, { t: 'kicked' });
      detach(kickedWs);
      kickedWs.close();
      return;
    }

    case 'start': {
      if (!room || room.started || !isHost(ws)) return;
      if (room.seats.length < MIN_SEATS) return sendError(ws, 'You need at least 2 players (bots count).');

      room.started = true;
      room.seed = crypto.randomInt(1, 2147483647);
      room.startMessage = {
        t: 'start',
        seed: room.seed,
        settings: room.settings,
        seats: seatList(room),
      };
      // Everybody gets time to load the game scene before they are expected to report
      for (const p of room.seats) if (!p.isBot) p.lastAlive = Date.now() + START_GRACE_MS;
      room.seats.forEach((p, i) => { if (p.ws) send(p.ws, { ...room.startMessage, you: i }); });
      console.log(`[${room.code}] game started with ${room.seats.length} seats`);
      return;
    }

    // Game commands are numbered and sent to everybody, in one order
    case 'cmd': {
      if (!room || !room.started || !player) return;
      const cmd = String(msg.cmd || '');
      if (cmd.length === 0 || cmd.length > 64) return;
      if (room.log.length >= MAX_LOG) return;

      let seat = room.seats.indexOf(player);                 // the server decides who sent it
      if (Number.isInteger(msg.asSeat) && msg.asSeat >= 0 && msg.asSeat !== seat) {
        if (room.host !== player) return;                    // only the host may act for others
        const target = room.seats[msg.asSeat];
        if (!target) return;
        const isTimeout = cmd.startsWith('7,');              // the host ends a turn when the clock runs out
        const hostMayPlay = target.isBot || !target.ws;      // bots and players who left
        if (!isTimeout && !hostMayPlay) return;
        seat = msg.asSeat;
      }

      const entry = { seq: room.log.length, seat, cmd };
      room.log.push(entry);
      broadcast(room, { t: 'cmd', seq: entry.seq, seat: entry.seat, cmd: entry.cmd });
      return;
    }

    // The host sends a fingerprint of its game now and then. Everybody else compares it with their own.
    case 'sync': {
      if (!room || !room.started || room.host !== player) return;
      const out = { t: 'sync', seq: msg.seq | 0, hash: msg.hash | 0 };
      for (const p of room.seats) if (p.ws && p !== player) send(p.ws, out);
      return;
    }

    default:
      return;
  }
}

// ---------- the server ----------
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Not A Parasite relay is running. Rooms: ' + rooms.size + '\n');
});

const wss = new WebSocketServer({
  server,
  maxPayload: 16 * 1024,
  // The Unity Editor sends no Origin. Browsers do, and ALLOWED_ORIGINS (optional) limits them.
  verifyClient: (info) => ALLOWED_ORIGINS.length === 0 || !info.origin || ALLOWED_ORIGINS.includes(info.origin),
});

wss.on('connection', (ws, req) => {
  // One address may not open an unlimited number of connections
  const ip = clientIp(req);
  const open = (connectionsPerIp.get(ip) || 0) + 1;
  if (open > MAX_CONNECTIONS_PER_IP) {
    ws.close(1013, 'Too many connections');
    return;
  }
  connectionsPerIp.set(ip, open);

  ws.isAlive = true;
  ws.room = null;
  ws.player = null;
  ws.windowStart = Date.now();
  ws.count = 0;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    const now = Date.now();                              // simple flood protection
    if (now - ws.windowStart > 1000) { ws.windowStart = now; ws.count = 0; }
    if (++ws.count > 30) return;

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (!msg || typeof msg.t !== 'string') return;

    try { handle(ws, msg); } catch (err) { console.error('handler error', err); }
  });

  ws.on('close', () => {
    detach(ws);
    const left = (connectionsPerIp.get(ip) || 1) - 1;
    if (left <= 0) connectionsPerIp.delete(ip); else connectionsPerIp.set(ip, left);
  });

  ws.on('error', () => {});
});

// Drop connections that stopped answering
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Once a second: send heartbeats, and hand over the host's duties if the host's game window stopped running
let tick = 0;
setInterval(() => {
  tick++;
  const now = Date.now();

  for (const room of rooms.values()) {
    if (!room.started) continue;

    // Players notice a dead connection quickly when these stop arriving
    if (tick % 5 === 0) for (const p of room.seats) if (p.ws) send(p.ws, { t: 'hb' });

    // The host plays for bots and runs the turn clock. If their game stopped (hidden tab, frozen window), hand over.
    if (!room.host || now - room.host.lastAlive < HOST_STALE_MS) continue;

    const fresh = room.seats.filter(p => !p.isBot && p.ws && p !== room.host && now - p.lastAlive < FRESH_MS);
    if (fresh.length === 0) continue;

    room.host = fresh[0];                                // the lowest seat among the players who are running
    console.log(`[${room.code}] host duties moved to ${room.host.name}`);
    sendLobby(room);
  }
}, 1000);

// Remove rooms that are too old or were abandoned
setInterval(() => {
  const now = Date.now();
  for (const room of [...rooms.values()]) {
    const tooOld = now - room.createdAt > MAX_ROOM_MS;
    const abandoned = room.emptySince !== null && now - room.emptySince > EMPTY_ROOM_MS;
    if (tooOld || abandoned) closeRoom(room);
  }
}, 60 * 1000);

server.listen(PORT, () => console.log(`Relay listening on port ${PORT}`));
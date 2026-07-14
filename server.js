const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const { createServer } = require('http');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const server = createServer(app);
const wss = new WebSocketServer({ server });

const rooms = new Map();
const clients = new Map();

// ── Categories ───────────────────────────────────────────────────────
const CATS = [
  {id:'geography',  name:'ภูมิศาสตร์',    icon:'🌍'},
  {id:'history',    name:'ประวัติศาสตร์',  icon:'📜'},
  {id:'science',    name:'วิทยาศาสตร์',   icon:'🔬'},
  {id:'sports',     name:'กีฬา',          icon:'⚽'},
  {id:'entertain',  name:'บันเทิง',        icon:'🎬'},
  {id:'food',       name:'อาหาร',         icon:'🍜'},
  {id:'tech',       name:'เทคโนโลยี',     icon:'💻'},
  {id:'anime',      name:'อนิเมะ',        icon:'🎌'},
  {id:'music',      name:'ดนตรี',         icon:'🎵'},
  {id:'thailand',   name:'ความรู้ไทย',    icon:'🇹🇭'},
  {id:'math',       name:'คณิตศาสตร์',    icon:'🔢'},
  {id:'general',    name:'ความรู้ทั่วไป', icon:'🧠'},
  {id:'geo2',       name:'ภูมิศาสตร์ II', icon:'🗺️'},
  {id:'sci2',       name:'วิทย์ II',      icon:'⚗️'},
  {id:'tech2',      name:'เทคโน II',      icon:'🖥️'},
  {id:'culture',    name:'วัฒนธรรมโลก',  icon:'🏛️'},
  {id:'space',      name:'อวกาศ',         icon:'🚀'},
  {id:'nature',     name:'ธรรมชาติ',      icon:'🌿'},
  {id:'physics',    name:'ฟิสิกส์',       icon:'⚡'},
  {id:'chemistry',  name:'เคมี',          icon:'🧪'},
  {id:'biology',    name:'ชีววิทยา',      icon:'🧬'},
  {id:'economics',  name:'เศรษฐศาสตร์',  icon:'📈'},
  {id:'philosophy', name:'ปรัชญา',        icon:'🤔'},
  {id:'programming',name:'โปรแกรมมิง',   icon:'👨‍💻'},
];

// ── Draft config by player count ─────────────────────────────────────
// totalRounds = จำนวน picks = จำนวนหัวข้อที่ใช้เล่น
// แต่ละรอบ = 1 ban + 1 pick ต่อผู้เล่น
// ต้องมี category เหลือพอ = totalRounds * 2 + (bans ทั้งหมด)
// 2 คน: 5 picks, 5 bans => ต้องการ 10 cats ✓
// 3 คน: 4 picks, 4 bans => ต้องการ 8 cats ✓
// 4 คน: 3 picks, 4 bans => ต้องการ 7 cats ✓  (ลด picks เพราะ 4คน×3=12ข้อก็เยอะพอ)
// 5-6 คน: 3 picks, 3 bans => ต้องการ 6 cats ✓
function getDraftConfig(numPlayers) {
  if (numPlayers <= 2) return { totalRounds: 5, bansPerRound: 1 };
  if (numPlayers === 3) return { totalRounds: 4, bansPerRound: 1 };
  if (numPlayers === 4) return { totalRounds: 3, bansPerRound: 1 };
  return { totalRounds: 3, bansPerRound: 1 }; // 5-6 players
}

// ── Helpers ──────────────────────────────────────────────────────────
function makeCode() {
  const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({length:5}, () => c[Math.floor(Math.random()*c.length)]).join('');
}
function send(ws, obj) {
  if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}
function broadcastAll(code, obj) {
  for (const [ws, info] of clients) {
    if (info.roomCode === code && ws.readyState === WebSocket.OPEN)
      ws.send(JSON.stringify(obj));
  }
}

// ── Shuffle array ─────────────────────────────────────────────────────
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ── Draft helpers ─────────────────────────────────────────────────────
function initDraft(room) {
  const { totalRounds } = getDraftConfig(room.players.length);
  room.bans = [];
  room.picks = [];
  room.draftPlayerIdx = 0;
  room.draftSubAction = 'ban';  // each turn: ban first, then pick
  room.totalRounds = totalRounds;
  // Shuffle player order for draft
  room.draftOrder = shuffle(room.players.map(p => p.id));
}

function currentDraftPlayerId(room) {
  return room.draftOrder[room.draftPlayerIdx % room.draftOrder.length];
}

function advanceDraft(room) {
  if (room.draftSubAction === 'ban') {
    // same player now picks
    room.draftSubAction = 'pick';
  } else {
    // move to next player, start with ban
    room.draftPlayerIdx++;
    room.draftSubAction = 'ban';
  }
}

function isDraftDone(room) {
  return room.picks.length >= room.totalRounds;
}

// ── Room snapshot ─────────────────────────────────────────────────────
function roomSnapshot(room) {
  return {
    type: 'room_state',
    room: {
      code: room.code,
      hostId: room.hostId,
      players: room.players,
      phase: room.phase,
      settings: room.settings,
      bans: room.bans,
      picks: room.picks,
      draftPlayerIdx: room.draftPlayerIdx,
      draftSubAction: room.draftSubAction,
      draftOrder: room.draftOrder,
      totalRounds: room.totalRounds,
      questionIndex: room.questionIndex,
      totalQuestions: room.picks.length * room.settings.qPerCat,
      scores: room.scores,
      answererIdx: room.answererIdx,
      hasAnswered: room.hasAnswered,
      revealData: room.revealData,
      items: room.items,
    }
  };
}

// ── Answerer ──────────────────────────────────────────────────────────
function getCurrentAnswerer(room) {
  return room.players[room.answererIdx % room.players.length];
}

// ── WebSocket ─────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const info = clients.get(ws);

    // ── CREATE ROOM ──────────────────────────────────────────────────
    if (msg.type === 'create_room') {
      const playerId = uuidv4();
      let code;
      do { code = makeCode(); } while (rooms.has(code));

      const room = {
        code,
        hostId: playerId,
        players: [{ id: playerId, name: msg.playerName, avatar: msg.avatar || '😀', online: true }],
        phase: 'lobby',
        settings: { qPerCat: 3, timePerQ: 20, apiKey: '' },
        bans: [], picks: [],
        draftPlayerIdx: 0,
        draftSubAction: 'ban',
        draftOrder: [playerId],
        totalRounds: 5,
        questionIndex: 0,
        answererIdx: 0,
        hasAnswered: false,
        scores: { [playerId]: 0 },
        items: { [playerId]: [] },
        revealData: null,
        currentQuestion: null,
        prevQuestions: {},
        usedQ: {},
        timerHandle: null,
      };
      rooms.set(code, room);
      clients.set(ws, { roomCode: code, playerId });
      send(ws, { type: 'joined', playerId, code });
      send(ws, roomSnapshot(room));
      return;
    }

    // ── JOIN ROOM ─────────────────────────────────────────────────────
    if (msg.type === 'join_room') {
      const code = msg.code?.toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'ไม่พบห้องรหัส ' + msg.code });
      if (room.phase !== 'lobby') return send(ws, { type: 'error', message: 'เกมเริ่มแล้ว' });
      if (room.players.length >= 6) return send(ws, { type: 'error', message: 'ห้องเต็ม (สูงสุด 6 คน)' });

      const playerId = uuidv4();
      room.players.push({ id: playerId, name: msg.playerName, avatar: msg.avatar || '😀', online: true });
      room.scores[playerId] = 0;
      room.items[playerId] = [];
      clients.set(ws, { roomCode: code, playerId });
      send(ws, { type: 'joined', playerId, code });
      broadcastAll(code, roomSnapshot(room));
      broadcastAll(code, { type: 'toast', message: `${msg.avatar} ${msg.playerName} เข้าร่วมห้อง` });
      return;
    }

    if (!info) return;
    const room = rooms.get(info.roomCode);
    if (!room) return;
    const isHost = room.hostId === info.playerId;

    // ── UPDATE SETTINGS ───────────────────────────────────────────────
    if (msg.type === 'update_settings' && isHost) {
      Object.assign(room.settings, msg.settings);
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    // ── START DRAFT ───────────────────────────────────────────────────
    if (msg.type === 'start_draft' && isHost) {
      if (room.players.length < 2) return send(ws, { type: 'error', message: 'ต้องมีผู้เล่นอย่างน้อย 2 คน' });
      room.phase = 'draft';
      initDraft(room);
      const cfg = getDraftConfig(room.players.length);
      // Notify how many rounds
      broadcastAll(room.code, {
        type: 'toast',
        message: `🎮 Draft เริ่ม! ${room.players.length} คน → ${cfg.totalRounds} หัวข้อ | ลำดับสุ่มแล้ว`
      });
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    // ── DRAFT ACTION ──────────────────────────────────────────────────
    if (msg.type === 'draft_action') {
      if (room.phase !== 'draft') return;
      const curId = currentDraftPlayerId(room);
      if (curId !== info.playerId) return; // ไม่ใช่ตาของคุณ

      const { catId } = msg;
      if (room.bans.includes(catId) || room.picks.includes(catId)) return;

      const action = room.draftSubAction;
      if (action === 'ban') room.bans.push(catId);
      else room.picks.push(catId);

      broadcastAll(room.code, { type: 'draft_action_done', action, catId, byPlayerId: info.playerId });
      advanceDraft(room);

      if (isDraftDone(room)) {
        setTimeout(() => {
          room.phase = 'game';
          room.questionIndex = 0;
          room.answererIdx = 0;
          room.players.forEach(p => { room.scores[p.id] = 0; });
          broadcastAll(room.code, roomSnapshot(room));
          loadNextQuestion(room);
        }, 1400);
      } else {
        broadcastAll(room.code, roomSnapshot(room));
      }
      return;
    }

    // ── SUBMIT ANSWER ─────────────────────────────────────────────────
    if (msg.type === 'submit_answer') {
      if (room.phase !== 'game' || !room.currentQuestion) return;
      if (room.hasAnswered) return;
      const answerer = getCurrentAnswerer(room);
      if (answerer.id !== info.playerId) return; // ไม่ใช่ตาตอบ

      room.hasAnswered = true;
      clearTimeout(room.timerHandle);

      const elapsed = (Date.now() - room.questionStartTime) / 1000;
      const { answerIndex } = msg;
      const correct = room.currentQuestion.answer;
      const isOk = answerIndex === correct;
      const bonus = isOk ? Math.max(0, Math.floor((1 - elapsed / room.settings.timePerQ) * 5)) : 0;
      if (isOk) room.scores[info.playerId] = (room.scores[info.playerId] || 0) + 10 + bonus;

      room.revealData = {
        correctAnswer: correct,
        explanation: room.currentQuestion.explanation || '',
        answerIndex,
        answererId: info.playerId,
        isOk,
        score: isOk ? 10 + bonus : 0,
        scores: { ...room.scores },
      };
      broadcastAll(room.code, { type: 'reveal_answer', ...room.revealData });
      scheduleAutoAdvance(room);
      return;
    }

    // ── HOST SENDS OFFLINE QUESTION ───────────────────────────────────
    if (msg.type === 'host_question_ready' && isHost) {
      if (!room.currentQuestion || room.currentQuestion.answer !== -1) return;
      const answerer = getCurrentAnswerer(room);
      room.currentQuestion = {
        question: msg.question,
        choices: msg.choices,
        answer: msg.answer,
        explanation: msg.explanation || '',
        catId: msg.catId,
        catName: msg.catName,
      };
      room.questionStartTime = Date.now();
      broadcastAll(room.code, {
        type: 'new_question',
        question: { question: msg.question, choices: msg.choices, catName: msg.catName },
        questionIndex: room.questionIndex,
        totalQuestions: room.picks.length * room.settings.qPerCat,
        scores: room.scores,
        timePerQ: room.settings.timePerQ,
        catId: msg.catId,
        answererId: answerer.id,
        answererName: answerer.name,
        answererAvatar: answerer.avatar,
      });
      scheduleTimeUp(room, answerer);
      return;
    }

    // ── USE ITEM (skeleton) ───────────────────────────────────────────
    if (msg.type === 'use_item') {
      const { itemId, targetId } = msg;
      const playerItems = room.items[info.playerId] || [];
      if (!playerItems.includes(itemId)) return;
      room.items[info.playerId] = playerItems.filter(i => i !== itemId);
      if (itemId === 'steal_points' && targetId && (room.scores[targetId] || 0) >= 5) {
        room.scores[targetId] -= 5;
        room.scores[info.playerId] = (room.scores[info.playerId] || 0) + 5;
      }
      broadcastAll(room.code, { type: 'item_effect', itemId, byPlayerId: info.playerId, targetId, scores: room.scores });
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    // ── RESTART ───────────────────────────────────────────────────────
    if (msg.type === 'restart' && isHost) {
      clearTimeout(room.timerHandle);
      room.phase = 'lobby';
      room.bans = []; room.picks = [];
      room.questionIndex = 0; room.answererIdx = 0; room.hasAnswered = false;
      room.players.forEach(p => { room.scores[p.id] = 0; room.items[p.id] = []; });
      room.revealData = null; room.currentQuestion = null;
      room.prevQuestions = {}; room.usedQ = {};
      initDraft(room);
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    if (msg.type === 'ping') send(ws, { type: 'pong' });
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (!info) return;
    const room = rooms.get(info.roomCode);
    if (room) {
      const p = room.players.find(p => p.id === info.playerId);
      if (p) {
        p.online = false;
        broadcastAll(room.code, roomSnapshot(room));
        broadcastAll(room.code, { type: 'toast', message: `${p.avatar} ${p.name} ออกจากห้อง` });
      }
      if (room.players.every(p => !p.online)) {
        setTimeout(() => {
          if (rooms.has(room.code) && rooms.get(room.code).players.every(p => !p.online))
            rooms.delete(room.code);
        }, 300000);
      }
    }
    clients.delete(ws);
  });
});

// ── Game logic ─────────────────────────────────────────────────────────
async function loadNextQuestion(room) {
  room.currentQuestion = null;
  room.hasAnswered = false;
  room.revealData = null;
  broadcastAll(room.code, { type: 'loading_question' });

  const catIdx = Math.floor(room.questionIndex / room.settings.qPerCat);
  const catId = room.picks[catIdx];
  const cat = CATS.find(c => c.id === catId) || CATS[0];
  const answerer = getCurrentAnswerer(room);

  if (room.settings.apiKey) {
    try {
      const prev = room.prevQuestions[catId] || [];
      const avoidPart = prev.length ? `\nห้ามซ้ำกับ:\n- ${prev.slice(-15).join('\n- ')}` : '';
      const prompt = `สร้างคำถามภาษาไทยหมวด "${cat.icon} ${cat.name}" 1 ข้อ พร้อม 4 ตัวเลือก ระดับปานกลาง${avoidPart}\nตอบ JSON เท่านั้น: {"question":"...","choices":["ก. ...","ข. ...","ค. ...","ง. ..."],"answer":0,"explanation":"..."}`;
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': room.settings.apiKey, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 600, messages: [{ role: 'user', content: prompt }] }),
      });
      const data = await res.json();
      if (res.ok) {
        const q = JSON.parse(data.content[0].text.trim().replace(/```json|```/g, '').trim());
        if (!room.prevQuestions[catId]) room.prevQuestions[catId] = [];
        room.prevQuestions[catId].push(q.question);
        room.currentQuestion = { ...q, catName: `${cat.icon} ${cat.name}`, catId };
        room.questionStartTime = Date.now();
        broadcastAll(room.code, {
          type: 'new_question',
          question: { question: q.question, choices: q.choices, catName: room.currentQuestion.catName },
          questionIndex: room.questionIndex,
          totalQuestions: room.picks.length * room.settings.qPerCat,
          scores: room.scores, timePerQ: room.settings.timePerQ,
          catId, answererId: answerer.id, answererName: answerer.name, answererAvatar: answerer.avatar,
        });
        scheduleTimeUp(room, answerer);
        return;
      }
    } catch (e) { console.log('API err, offline fallback'); }
  }

  // Offline: ask host to generate
  room.currentQuestion = { question: '__need_host__', choices: [], answer: -1, explanation: '', catId, catName: `${cat.icon} ${cat.name}` };
  broadcastAll(room.code, {
    type: 'request_host_question',
    catId, catName: `${cat.icon} ${cat.name}`,
    questionIndex: room.questionIndex,
    totalQuestions: room.picks.length * room.settings.qPerCat,
    scores: room.scores, timePerQ: room.settings.timePerQ,
    answererId: answerer.id, answererName: answerer.name, answererAvatar: answerer.avatar,
  });
}

function scheduleTimeUp(room, answerer) {
  clearTimeout(room.timerHandle);
  room.timerHandle = setTimeout(() => {
    if (room.hasAnswered) return;
    room.hasAnswered = true;
    room.revealData = {
      correctAnswer: room.currentQuestion?.answer,
      explanation: room.currentQuestion?.explanation || '',
      answerIndex: -1,
      answererId: answerer.id,
      isOk: false, score: 0,
      scores: { ...room.scores },
    };
    broadcastAll(room.code, { type: 'reveal_answer', ...room.revealData });
    scheduleAutoAdvance(room);
  }, room.settings.timePerQ * 1000 + 800);
}

const REVEAL_WAIT = 5; // seconds before auto-advance

function scheduleAutoAdvance(room) {
  broadcastAll(room.code, { type: 'auto_advance_countdown', seconds: REVEAL_WAIT });
  room.timerHandle = setTimeout(() => doAutoAdvance(room), REVEAL_WAIT * 1000);
}

function doAutoAdvance(room) {
  room.questionIndex++;
  room.answererIdx++;
  room.hasAnswered = false;
  room.revealData = null;
  const total = room.picks.length * room.settings.qPerCat;
  if (room.questionIndex >= total) {
    room.phase = 'results';
    broadcastAll(room.code, roomSnapshot(room));
  } else {
    broadcastAll(room.code, roomSnapshot(room));
    loadNextQuestion(room);
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`QuizBan :${PORT}`));

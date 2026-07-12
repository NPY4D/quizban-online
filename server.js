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

const CATS = [
  {id:'geography', name:'ภูมิศาสตร์',    icon:'🌍'},
  {id:'history',   name:'ประวัติศาสตร์',  icon:'📜'},
  {id:'science',   name:'วิทยาศาสตร์',    icon:'🔬'},
  {id:'sports',    name:'กีฬา',           icon:'⚽'},
  {id:'entertain', name:'บันเทิง',         icon:'🎬'},
  {id:'food',      name:'อาหาร',          icon:'🍜'},
  {id:'tech',      name:'เทคโนโลยี',      icon:'💻'},
  {id:'anime',     name:'อนิเมะ',         icon:'🎌'},
  {id:'music',     name:'ดนตรี',          icon:'🎵'},
  {id:'thailand',  name:'ความรู้ไทย',     icon:'🇹🇭'},
  {id:'math',      name:'คณิตศาสตร์',     icon:'🔢'},
  {id:'general',   name:'ความรู้ทั่วไป',  icon:'🧠'},
  {id:'geo2',      name:'ภูมิศาสตร์ II',  icon:'🗺️'},
  {id:'sci2',      name:'วิทย์ II',       icon:'⚗️'},
  {id:'tech2',     name:'เทคโน II',       icon:'🖥️'},
  {id:'culture',   name:'วัฒนธรรมโลก',   icon:'🏛️'},
  {id:'space',     name:'อวกาศ',          icon:'🚀'},
  {id:'nature',    name:'ธรรมชาติ',       icon:'🌿'},
];

// ── helpers ──────────────────────────────────────────────────────────
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

// ── Draft state helpers ───────────────────────────────────────────────
// Draft structure:
//   draftPlayerIdx  : whose turn it is (index into players[])
//   draftSubAction  : 'ban' | 'pick'  (what that player must do next)
//   draftRound      : which round (0-based), each round = one full rotation
//   totalRounds     : 5  → 5 picks + 5 bans total
//
// Each player's turn = ban 1 THEN pick 1, then next player

function initDraft(room) {
  room.bans = [];
  room.picks = [];
  room.draftPlayerIdx = 0;
  room.draftSubAction = 'ban';   // every turn starts with ban
  room.draftRound = 0;
  room.totalRounds = 5;
}

function advanceDraft(room) {
  if (room.draftSubAction === 'ban') {
    // same player now picks
    room.draftSubAction = 'pick';
  } else {
    // pick done → next player, start with ban again
    room.draftPlayerIdx = (room.draftPlayerIdx + 1) % room.players.length;
    room.draftSubAction = 'ban';
    // check if we finished enough rounds
    // picks.length drives completion
  }
}

function isDraftDone(room) {
  return room.picks.length >= room.totalRounds;
}

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
      draftRound: room.draftRound,
      totalRounds: room.totalRounds,
      currentQuestion: room.currentQuestion ? {
        question: room.currentQuestion.question,
        choices: room.currentQuestion.choices,
        catName: room.currentQuestion.catName,
      } : null,
      questionIndex: room.questionIndex,
      totalQuestions: room.picks.length * room.settings.qPerCat,
      scores: room.scores,
      roundAnswers: room.roundAnswers,
      revealData: room.revealData,
    }
  };
}

// ── WebSocket ─────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  ws.on('message', async (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const info = clients.get(ws);

    // CREATE ROOM
    if (msg.type === 'create_room') {
      const playerId = uuidv4();
      let code; do { code = makeCode(); } while (rooms.has(code));
      const room = {
        code, hostId: playerId,
        players: [{ id: playerId, name: msg.playerName, avatar: msg.avatar||'😀', online: true }],
        phase: 'lobby',
        settings: { qPerCat: 3, timePerQ: 20, apiKey: '' },
        bans: [], picks: [],
        draftPlayerIdx: 0, draftSubAction: 'ban',
        draftRound: 0, totalRounds: 5,
        questionIndex: 0,
        scores: { [playerId]: 0 },
        roundAnswers: {}, revealData: null,
        currentQuestion: null,
        prevQuestions: {}, usedQ: {},
        timerHandle: null,
      };
      rooms.set(code, room);
      clients.set(ws, { roomCode: code, playerId });
      send(ws, { type: 'joined', playerId, code });
      send(ws, roomSnapshot(room));
      return;
    }

    // JOIN ROOM
    if (msg.type === 'join_room') {
      const code = msg.code?.toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'ไม่พบห้องรหัส ' + msg.code });
      if (room.phase !== 'lobby') return send(ws, { type: 'error', message: 'เกมเริ่มแล้ว' });
      if (room.players.length >= 6) return send(ws, { type: 'error', message: 'ห้องเต็ม (สูงสุด 6 คน)' });
      const playerId = uuidv4();
      room.players.push({ id: playerId, name: msg.playerName, avatar: msg.avatar||'😀', online: true });
      room.scores[playerId] = 0;
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

    // UPDATE SETTINGS
    if (msg.type === 'update_settings' && isHost) {
      Object.assign(room.settings, msg.settings);
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    // START DRAFT
    if (msg.type === 'start_draft' && isHost) {
      if (room.players.length < 2) return send(ws, { type: 'error', message: 'ต้องมีผู้เล่นอย่างน้อย 2 คน' });
      room.phase = 'draft';
      initDraft(room);
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    // DRAFT ACTION
    if (msg.type === 'draft_action') {
      if (room.phase !== 'draft') return;
      const curPlayer = room.players[room.draftPlayerIdx];
      if (!curPlayer || curPlayer.id !== info.playerId) return;
      const { catId } = msg;
      if (room.bans.includes(catId) || room.picks.includes(catId)) return;

      const action = room.draftSubAction;
      if (action === 'ban') room.bans.push(catId);
      else room.picks.push(catId);

      // broadcast the action so clients can animate
      broadcastAll(room.code, { type: 'draft_action_done', action, catId, byPlayerId: info.playerId });

      advanceDraft(room);

      if (isDraftDone(room)) {
        setTimeout(() => {
          room.phase = 'game';
          room.questionIndex = 0;
          room.players.forEach(p => { room.scores[p.id] = 0; });
          broadcastAll(room.code, roomSnapshot(room));
          loadNextQuestion(room);
        }, 1400);
      } else {
        broadcastAll(room.code, roomSnapshot(room));
      }
      return;
    }

    // SUBMIT ANSWER
    if (msg.type === 'submit_answer') {
      if (room.phase !== 'game' || !room.currentQuestion) return;
      if (room.roundAnswers[info.playerId] !== undefined) return;
      const elapsed = (Date.now() - room.questionStartTime) / 1000;
      const { answerIndex } = msg;
      const correct = room.currentQuestion.answer;
      const isOk = answerIndex === correct;
      const bonus = isOk ? Math.max(0, Math.floor((1 - elapsed/room.settings.timePerQ)*5)) : 0;
      room.roundAnswers[info.playerId] = { answerIndex, isOk, score: isOk ? 10+bonus : 0 };
      if (isOk) room.scores[info.playerId] = (room.scores[info.playerId]||0) + 10 + bonus;
      broadcastAll(room.code, { type: 'player_answered', playerId: info.playerId,
        totalAnswered: Object.keys(room.roundAnswers).length, totalPlayers: room.players.length });
      if (Object.keys(room.roundAnswers).length >= room.players.length) {
        clearTimeout(room.timerHandle); doReveal(room);
      }
      return;
    }

    // NEXT QUESTION
    if (msg.type === 'next_question' && isHost) {
      room.questionIndex++;
      const total = room.picks.length * room.settings.qPerCat;
      if (room.questionIndex >= total) {
        room.phase = 'results';
        broadcastAll(room.code, roomSnapshot(room));
      } else {
        loadNextQuestion(room);
      }
      return;
    }

    // RESTART
    if (msg.type === 'restart' && isHost) {
      clearTimeout(room.timerHandle);
      room.phase = 'lobby';
      initDraft(room);
      room.questionIndex = 0;
      room.players.forEach(p => { room.scores[p.id] = 0; });
      room.roundAnswers = {}; room.revealData = null;
      room.currentQuestion = null; room.prevQuestions = {}; room.usedQ = {};
      broadcastAll(room.code, roomSnapshot(room));
      return;
    }

    if (msg.type === 'ping') send(ws, { type: 'pong' });
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (info) {
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
    }
  });
});

// ── Game logic ────────────────────────────────────────────────────────
async function loadNextQuestion(room) {
  room.currentQuestion = null;
  room.roundAnswers = {};
  room.revealData = null;
  broadcastAll(room.code, { type: 'loading_question' });

  const catIdx = Math.floor(room.questionIndex / room.settings.qPerCat);
  const catId = room.picks[catIdx];
  const cat = CATS.find(c => c.id === catId) || CATS[0];

  let question;
  if (room.settings.apiKey) {
    try {
      const prev = room.prevQuestions[catId] || [];
      const avoidPart = prev.length ? `\nห้ามซ้ำกับ:\n- ${prev.slice(-15).join('\n- ')}` : '';
      const prompt = `สร้างคำถามภาษาไทยหมวด "${cat.icon} ${cat.name}" 1 ข้อ พร้อม 4 ตัวเลือก ระดับปานกลาง${avoidPart}\nตอบ JSON เท่านั้น: {"question":"...","choices":["ก. ...","ข. ...","ค. ...","ง. ..."],"answer":0,"explanation":"..."}`;
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method:'POST',
        headers:{'Content-Type':'application/json','x-api-key':room.settings.apiKey,'anthropic-version':'2023-06-01'},
        body: JSON.stringify({ model:'claude-sonnet-4-6', max_tokens:600, messages:[{role:'user',content:prompt}] }),
      });
      const data = await res.json();
      if (res.ok) {
        question = JSON.parse(data.content[0].text.trim().replace(/```json|```/g,'').trim());
        if (!room.prevQuestions[catId]) room.prevQuestions[catId] = [];
        room.prevQuestions[catId].push(question.question);
      } else throw new Error('api fail');
    } catch { question = { question:'__offline__', choices:[], answer:0, explanation:'', _offline:true, catId }; }
  } else {
    question = { question:'__offline__', choices:[], answer:0, explanation:'', _offline:true, catId };
  }

  question.catName = `${cat.icon} ${cat.name}`;
  question.catId = catId;
  room.currentQuestion = question;
  room.questionStartTime = Date.now();

  broadcastAll(room.code, {
    type: 'new_question',
    question: { question: question.question, choices: question.choices, catName: question.catName },
    questionIndex: room.questionIndex,
    totalQuestions: room.picks.length * room.settings.qPerCat,
    scores: room.scores,
    timePerQ: room.settings.timePerQ,
    catId: catId,
  });

  room.timerHandle = setTimeout(() => doReveal(room), room.settings.timePerQ * 1000 + 600);
}

function doReveal(room) {
  clearTimeout(room.timerHandle);
  if (!room.currentQuestion || room.revealData) return;
  const q = room.currentQuestion;
  room.revealData = { correctAnswer: q.answer, explanation: q.explanation, scores: {...room.scores}, roundAnswers: room.roundAnswers };
  broadcastAll(room.code, { type: 'reveal_answer', ...room.revealData });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`QuizBan :${PORT}`));

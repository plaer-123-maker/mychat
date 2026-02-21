const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// ЛИМИТ 50 МБ
const io = new Server(server, { maxHttpBufferSize: 5e7, cors: { origin: "*" } });

app.use(express.static('public'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

const ADMIN_LOGIN = 'pekka';

async function safeQuery(text, params) {
  try { return await pool.query(text, params); } 
  catch (e) { console.error("SQL Error:", e.message); return null; }
}

async function initDB() {
  console.log('--- RE-INIT DB ---');
  // СБРОС ТАБЛИЦ ДЛЯ ИСПРАВЛЕНИЯ ОШИБОК
  try {
    await pool.query('DROP TABLE IF EXISTS room_messages CASCADE');
    await pool.query('DROP TABLE IF EXISTS room_members CASCADE');
    await pool.query('DROP TABLE IF EXISTS rooms CASCADE');
    await pool.query('DROP TABLE IF EXISTS private_messages CASCADE');
    await pool.query('DROP TABLE IF EXISTS messages CASCADE');
  } catch(e) {}

  // СОЗДАНИЕ
  await safeQuery(`CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, login VARCHAR(50) UNIQUE, password VARCHAR(200), nickname VARCHAR(50), banned BOOLEAN DEFAULT false, muted_until BIGINT DEFAULT 0, role VARCHAR(20) DEFAULT 'user', token VARCHAR(200))`);
  await safeQuery(`CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, username VARCHAR(100), text TEXT, image TEXT, voice TEXT, file_data TEXT, file_name TEXT, type VARCHAR(20) DEFAULT 'text', timestamp BIGINT)`);
  await safeQuery(`CREATE TABLE IF NOT EXISTS private_messages (id SERIAL PRIMARY KEY, from_login VARCHAR(50), to_login VARCHAR(50), from_nickname VARCHAR(100), text TEXT, image TEXT, voice TEXT, file_data TEXT, file_name TEXT, type VARCHAR(20) DEFAULT 'text', timestamp BIGINT, read BOOLEAN DEFAULT false)`);
  await safeQuery(`CREATE TABLE IF NOT EXISTS rooms (id SERIAL PRIMARY KEY, name VARCHAR(100), type VARCHAR(20), owner_login VARCHAR(50), comments_enabled BOOLEAN DEFAULT true, timestamp BIGINT)`);
  await safeQuery(`CREATE TABLE IF NOT EXISTS room_members (id SERIAL PRIMARY KEY, room_id INT, user_login VARCHAR(50), role VARCHAR(20) DEFAULT 'member', UNIQUE(room_id, user_login))`);
  await safeQuery(`CREATE TABLE IF NOT EXISTS room_messages (id SERIAL PRIMARY KEY, room_id INT, user_login VARCHAR(50), username VARCHAR(100), text TEXT, image TEXT, voice TEXT, file_data TEXT, file_name TEXT, type VARCHAR(20) DEFAULT 'text', timestamp BIGINT)`);
  await safeQuery(`CREATE TABLE IF NOT EXISTS logs (id SERIAL PRIMARY KEY, action VARCHAR(50), username VARCHAR(100), detail TEXT, ip VARCHAR(50), timestamp BIGINT)`);

  try { await safeQuery("UPDATE users SET role='admin' WHERE login=$1", [ADMIN_LOGIN]); } catch(e) {}
  console.log('--- DB READY ---');
}
initDB();

const onlineUsers = new Map();
const socketUsers = new Map();

function getIP(socket) { return socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || 'unknown'; }
function isAdmin(socket) { return socket.userLogin === ADMIN_LOGIN || socket.userRole === 'admin' || socket.userRole === 'moderator'; }
function findSocketByLogin(login) { for (let [sid, info] of onlineUsers) { if (info.login === login) return socketUsers.get(sid); } return null; }

function sendOnlineToAll() {
  var list = []; for (let [sid, info] of onlineUsers) list.push({ nickname: info.nickname, login: info.login });
  for (let [sid] of onlineUsers) {
    var s = socketUsers.get(sid); if (!s) continue;
    if (s.userRole === 'admin' || s.userRole === 'moderator') s.emit('onlineUsers', { count: list.length, users: list, isAdmin: true });
    else s.emit('onlineUsers', { count: list.length, users: [], isAdmin: false });
  }
}

io.on('connection', (socket) => {
  const ip = getIP(socket);

  socket.on('autoLogin', async (token) => {
    if (!token) return socket.emit('authError', 'Нет токена');
    const res = await safeQuery('SELECT * FROM users WHERE token=$1', [token]);
    if (!res || res.rows.length === 0) return socket.emit('authError', 'Токен недействителен');
    const user = res.rows[0];
    if (user.banned) return socket.emit('authError', 'Бан');
    socket.username = user.nickname; socket.userLogin = user.login; socket.userRole = user.role;
    onlineUsers.set(socket.id, { nickname: user.nickname, login: user.login, ip }); socketUsers.set(socket.id, socket);
    socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login: user.login, token: token });
    sendOnlineToAll();
  });

  socket.on('register', async ({ login, password, nickname }) => {
    if (!login || !password || !nickname) return socket.emit('authError', 'Пусто');
    const exists = await safeQuery('SELECT id FROM users WHERE login=$1', [login]);
    if (exists && exists.rows.length > 0) return socket.emit('authError', 'Логин занят');
    const hash = await bcrypt.hash(password, 10);
    const role = login === ADMIN_LOGIN ? 'admin' : 'user';
    const token = crypto.randomBytes(32).toString('hex');
    await safeQuery('INSERT INTO users (login,password,nickname,role,token) VALUES ($1,$2,$3,$4,$5)', [login, hash, nickname, role, token]);
    socket.username = nickname; socket.userLogin = login; socket.userRole = role;
    onlineUsers.set(socket.id, { nickname, login, ip }); socketUsers.set(socket.id, socket);
    socket.emit('authSuccess', { nickname, role, login, token });
    sendOnlineToAll();
  });

  socket.on('login', async ({ login, password }) => {
    const res = await safeQuery('SELECT * FROM users WHERE login=$1', [login]);
    if (!res || res.rows.length === 0) return socket.emit('authError', 'Неверно');
    const user = res.rows[0];
    if (user.banned) return socket.emit('authError', 'Бан');
    if (!(await bcrypt.compare(password, user.password))) return socket.emit('authError', 'Неверно');
    socket.username = user.nickname; socket.userLogin = login; socket.userRole = user.role;
    const token = crypto.randomBytes(32).toString('hex');
    await safeQuery('UPDATE users SET token=$1 WHERE login=$2', [token, login]);
    onlineUsers.set(socket.id, { nickname: user.nickname, login, ip }); socketUsers.set(socket.id, socket);
    socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login, token });
    sendOnlineToAll();
  });

  // CALLS
  socket.on('callUser', (d) => { var s = findSocketByLogin(d.userToCall); if(s) s.emit('incomingCall', { signal: d.signalData, from: socket.userLogin, fromNickname: socket.username, callType: d.callType }); });
  socket.on('answerCall', (d) => { var s = findSocketByLogin(d.to); if(s) s.emit('callAccepted', d.signal); });
  socket.on('iceCandidate', (d) => { var s = findSocketByLogin(d.to); if(s) s.emit('iceCandidate', d.candidate); });
  socket.on('hangUp', (d) => { var s = findSocketByLogin(d.to); if(s) s.emit('callEnded'); });

  // MESSAGES
  socket.on('getGeneralHistory', async () => { if(!socket.userLogin) return; const res = await safeQuery('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200'); if(res) socket.emit('messageHistory', res.rows); });
  socket.on('chatMessage', async (d) => {
    if(!socket.username) return;
    const msg={username:socket.username, text:d.text||'', image:d.image, voice:d.voice, file_data:d.file_data, file_name:d.file_name, type:d.type||'text', timestamp:Date.now()};
    const res=await safeQuery('INSERT INTO messages (username,text,image,voice,file_data,file_name,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id', [msg.username,msg.text,msg.image,msg.voice,msg.file_data,msg.file_name,msg.type,msg.timestamp]);
    if(res){ msg.id=res.rows[0].id; io.emit('chatMessage', msg); }
  });

  // PRIVATE & ROOMS (Simplified for brevity but fully functional based on previous code)
  socket.on('getMyChats', async () => { if(!socket.userLogin) return; const res=await safeQuery('SELECT DISTINCT CASE WHEN from_login=$1 THEN to_login ELSE from_login END as other FROM private_messages WHERE from_login=$1 OR to_login=$1', [socket.userLogin]); if(!res) return socket.emit('myChats',[]); var users=await safeQuery('SELECT login,nickname FROM users WHERE login=ANY($1)', [res.rows.map(r=>r.other)]); if(users) socket.emit('myChats', users.rows.map(u=>({login:u.login, nickname:u.nickname, unread:0}))); });
  socket.on('getPrivateHistory', async (l) => { if(!socket.userLogin) return; const res=await safeQuery('SELECT * FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) ORDER BY timestamp ASC', [socket.userLogin, l]); if(res) socket.emit('privateHistory', {otherLogin:l, messages:res.rows}); });
  socket.on('privateMessage', async (d) => { if(!socket.userLogin) return; const msg={from_login:socket.userLogin, to_login:d.toLogin, from_nickname:socket.username, text:d.text||'', image:d.image, voice:d.voice, file_data:d.file_data, file_name:d.file_name, type:d.type||'text', timestamp:Date.now()}; const res=await safeQuery('INSERT INTO private_messages (from_login,to_login,from_nickname,text,image,voice,file_data,file_name,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id', [msg.from_login,msg.to_login,msg.from_nickname,msg.text,msg.image,msg.voice,msg.file_data,msg.file_name,msg.type,msg.timestamp]); if(res){ msg.id=res.rows[0].id; socket.emit('newPrivateMessage', msg); var t=findSocketByLogin(d.toLogin); if(t) t.emit('newPrivateMessage', msg); } });

  socket.on('createRoom', async (d) => { if(!socket.userLogin) return; const res=await safeQuery('INSERT INTO rooms (name,type,owner_login,timestamp) VALUES ($1,$2,$3,$4) RETURNING *', [d.name, d.type, socket.userLogin, Date.now()]); if(res && res.rows[0]){ var r=res.rows[0]; await safeQuery('INSERT INTO room_members (room_id,user_login,role) VALUES ($1,$2,$3)', [r.id, socket.userLogin, 'admin']); socket.join('room_'+r.id); socket.emit('roomCreated', r); } });
  socket.on('getMyRooms', async () => { if(!socket.userLogin) return; const res=await safeQuery('SELECT r.*, rm.role FROM rooms r JOIN room_members rm ON r.id=rm.room_id WHERE rm.user_login=$1', [socket.userLogin]); if(res) socket.emit('myRooms', res.rows); });
  socket.on('joinRoom', async (rid) => { if(!socket.userLogin) return; await safeQuery('INSERT INTO room_members (room_id,user_login,role) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [rid, socket.userLogin, 'member']); socket.join('room_'+rid); socket.emit('joinedRoom', rid); });
  socket.on('openRoom', async (rid) => { if(!socket.userLogin) return; const r=await safeQuery('SELECT * FROM rooms WHERE id=$1', [rid]); const m=await safeQuery('SELECT * FROM room_messages WHERE room_id=$1 ORDER BY timestamp ASC', [rid]); const mem=await safeQuery('SELECT * FROM room_members WHERE room_id=$1', [rid]); if(r && r.rows[0]) socket.emit('roomData', {room:r.rows[0], myRole:'member', messages:m?m.rows:[], members:mem?mem.rows:[]}); });
  socket.on('roomMessage', async (d) => { if(!socket.userLogin) return; const msg={room_id:d.roomId, user_login:socket.userLogin, username:socket.username, text:d.text||'', image:d.image, voice:d.voice, file_data:d.file_data, file_name:d.file_name, type:d.type||'text', timestamp:Date.now()}; const res=await safeQuery('INSERT INTO room_messages (room_id,user_login,username,text,image,voice,file_data,file_name,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id', [msg.room_id,msg.user_login,msg.username,msg.text,msg.image,msg.voice,msg.file_data,msg.file_name,msg.type,msg.timestamp]); if(res){ msg.id=res.rows[0].id; io.to('room_'+d.roomId).emit('roomNewMessage', msg); } });
  
  socket.on('searchUser', async (q) => { if(q) { const res=await safeQuery('SELECT login,nickname FROM users WHERE nickname ILIKE $1 LIMIT 10', ['%'+q+'%']); if(res) socket.emit('searchResults', res.rows); } });
  socket.on('searchRooms', async (q) => { if(q) { const res=await safeQuery('SELECT * FROM rooms WHERE name ILIKE $1 LIMIT 10', ['%'+q+'%']); if(res) socket.emit('roomSearchResults', res.rows); } });

  socket.on('disconnect', () => { onlineUsers.delete(socket.id); socketUsers.delete(socket.id); sendOnlineToAll(); });
});

server.listen(process.env.PORT || 3000, () => console.log('Server OK'));
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// ЛИМИТ 2 ГБ (2 * 1024 * 1024 * 1024)
const io = new Server(server, { maxHttpBufferSize: 2e9 });

app.use(express.static('public'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

const ADMIN_LOGIN = 'pekka';

async function initDB() {
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY, login VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(200) NOT NULL, nickname VARCHAR(50) NOT NULL,
    banned BOOLEAN DEFAULT false, muted_until BIGINT DEFAULT 0,
    role VARCHAR(20) DEFAULT 'user',
    token VARCHAR(200) DEFAULT NULL
  )`);
  
  await pool.query(`CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY, username VARCHAR(100) NOT NULL,
    text TEXT, image TEXT, voice TEXT,
    file_data TEXT, file_name TEXT,
    type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL
  )`);
  
  await pool.query(`CREATE TABLE IF NOT EXISTS private_messages (
    id SERIAL PRIMARY KEY, from_login VARCHAR(50) NOT NULL,
    to_login VARCHAR(50) NOT NULL, from_nickname VARCHAR(100),
    text TEXT, image TEXT, voice TEXT,
    file_data TEXT, file_name TEXT,
    type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL,
    read BOOLEAN DEFAULT false
  )`);

  await pool.query(`CREATE TABLE IF NOT EXISTS rooms (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL,
    owner_login VARCHAR(50) NOT NULL,
    comments_enabled BOOLEAN DEFAULT true,
    timestamp BIGINT NOT NULL
  )`);
  
  await pool.query(`CREATE TABLE IF NOT EXISTS room_members (
    id SERIAL PRIMARY KEY,
    room_id INT NOT NULL,
    user_login VARCHAR(50) NOT NULL,
    role VARCHAR(20) DEFAULT 'member',
    UNIQUE(room_id, user_login)
  )`);
  
  await pool.query(`CREATE TABLE IF NOT EXISTS room_messages (
    id SERIAL PRIMARY KEY,
    room_id INT NOT NULL,
    user_login VARCHAR(50) NOT NULL,
    username VARCHAR(100) NOT NULL,
    text TEXT, image TEXT, voice TEXT,
    file_data TEXT, file_name TEXT,
    type VARCHAR(20) DEFAULT 'text',
    timestamp BIGINT NOT NULL
  )`);
  
  await pool.query(`CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY, action VARCHAR(50) NOT NULL,
    username VARCHAR(100), detail TEXT, ip VARCHAR(50),
    timestamp BIGINT NOT NULL
  )`);

  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS muted_until BIGINT DEFAULT 0'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT \'user\''); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS token VARCHAR(200) DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE rooms ADD COLUMN IF NOT EXISTS comments_enabled BOOLEAN DEFAULT true'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_data TEXT'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS file_data TEXT'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS file_name TEXT'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS file_data TEXT'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS file_name TEXT'); } catch(e) {}

  try { await pool.query("UPDATE users SET role='admin' WHERE login=$1", [ADMIN_LOGIN]); } catch(e) {}
  console.log('Database ready');
}
initDB();

const onlineUsers = new Map();
const socketUsers = new Map();

function getIP(socket) {
  return socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || 'unknown';
}

async function addLog(action, username, detail, ip) {
  try { await pool.query('INSERT INTO logs (action,username,detail,ip,timestamp) VALUES ($1,$2,$3,$4,$5)',
    [action, username||'', detail||'', ip||'', Date.now()]); } catch(e) {}
}

function isAdmin(socket) {
  return socket.userLogin === ADMIN_LOGIN || socket.userRole === 'admin' || socket.userRole === 'moderator';
}

function isSuperAdmin(socket) { return socket.userLogin === ADMIN_LOGIN; }

function findSocketByLogin(login) {
  for (let [sid, info] of onlineUsers) {
    if (info.login === login) return socketUsers.get(sid);
  }
  return null;
}

function sendOnlineToAll() {
  var list = [];
  for (let [sid, info] of onlineUsers) list.push({ nickname: info.nickname, login: info.login });
  for (let [sid] of onlineUsers) {
    var s = socketUsers.get(sid);
    if (!s) continue;
    if (s.userRole === 'admin' || s.userRole === 'moderator') {
      s.emit('onlineUsers', { count: list.length, users: list, isAdmin: true });
    } else {
      s.emit('onlineUsers', { count: list.length, users: [], isAdmin: false });
    }
  }
}

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

io.on('connection', (socket) => {
  const ip = getIP(socket);

  socket.on('autoLogin', async (token) => {
    if (!token) return socket.emit('authError', 'Нет токена');
    try {
      const res = await pool.query('SELECT * FROM users WHERE token=$1', [token]);
      if (res.rows.length === 0) return socket.emit('authError', 'Токен недействителен');
      const user = res.rows[0];
      if (user.banned) return socket.emit('authError', 'Ваш аккаунт заблокирован');
      socket.username = user.nickname;
      socket.userLogin = user.login;
      socket.userRole = user.login === ADMIN_LOGIN ? 'admin' : (user.role || 'user');
      onlineUsers.set(socket.id, { nickname: user.nickname, login: user.login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login: user.login, token: token });
      sendOnlineToAll();
    } catch(e) { console.error(e); socket.emit('authError', 'Ошибка авто-входа'); }
  });

  socket.on('register', async ({ login, password, nickname }) => {
    try {
      if (!login || !password || !nickname) return socket.emit('authError', 'Заполни все поля');
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) return socket.emit('authError', 'Этот логин уже занят');
      const nickExists = await pool.query('SELECT id FROM users WHERE LOWER(nickname)=LOWER($1)', [nickname]);
      if (nickExists.rows.length > 0) return socket.emit('authError', 'Этот ник уже занят');
      const hash = await bcrypt.hash(password, 10);
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      const token = generateToken();
      await pool.query('INSERT INTO users (login,password,nickname,banned,muted_until,role,token) VALUES ($1,$2,$3,false,0,$4,$5)', [login, hash, nickname, role, token]);
      socket.username = nickname; socket.userLogin = login; socket.userRole = role;
      onlineUsers.set(socket.id, { nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role, login, token });
      sendOnlineToAll();
      await addLog('register', nickname, 'Registered', ip);
    } catch (e) { console.error(e); socket.emit('authError', 'Ошибка регистрации'); }
  });

  socket.on('login', async ({ login, password }) => {
    try {
      if (!login || !password) return socket.emit('authError', 'Заполни все поля');
      const res = await pool.query('SELECT * FROM users WHERE login=$1', [login]);
      if (res.rows.length === 0) return socket.emit('authError', 'Неверный логин или пароль');
      const user = res.rows[0];
      if (user.banned) return socket.emit('authError', 'Ваш аккаунт заблокирован');
      const valid = await bcrypt.compare(password, user.password);
      if (!valid) return socket.emit('authError', 'Неверный логин или пароль');
      socket.username = user.nickname; socket.userLogin = login;
      socket.userRole = login === ADMIN_LOGIN ? 'admin' : (user.role || 'user');
      const token = generateToken();
      await pool.query('UPDATE users SET token=$1 WHERE login=$2', [token, login]);
      onlineUsers.set(socket.id, { nickname: user.nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login, token });
      sendOnlineToAll();
      await addLog('login', user.nickname, 'Login', ip);
    } catch (e) { console.error(e); socket.emit('authError', 'Ошибка входа'); }
  });

  // WEBRTC
  socket.on('callUser', ({ userToCall, signalData, callType }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(userToCall);
    if (targetSocket) {
      targetSocket.emit('incomingCall', { signal: signalData, from: socket.userLogin, fromNickname: socket.username, callType: callType || 'video' });
    }
  });
  socket.on('answerCall', ({ signal, to }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(to);
    if (targetSocket) targetSocket.emit('callAccepted', signal);
  });
  socket.on('hangUp', ({ to }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(to);
    if (targetSocket) targetSocket.emit('callEnded');
  });
  socket.on('iceCandidate', ({ candidate, to }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(to);
    if (targetSocket) targetSocket.emit('iceCandidate', candidate);
  });

  // GENERAL
  socket.on('getGeneralHistory', async () => {
    if (!socket.userLogin) return;
    try {
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
    } catch(e) { console.error(e); }
  });

  socket.on('chatMessage', async (data) => {
    if (!socket.username) return;
    try {
      const u = await pool.query('SELECT muted_until FROM users WHERE login=$1', [socket.userLogin]);
      if (u.rows.length > 0 && u.rows[0].muted_until > Date.now()) return socket.emit('chatError', 'Вы замучены');
    } catch(e) {}
    
    const msg = { 
      username: socket.username, 
      text: data.text||'', image: data.image||null, voice: data.voice||null, 
      file_data: data.file_data||null, file_name: data.file_name||null,
      type: data.type||'text', timestamp: Date.now() 
    };

    try {
      const res = await pool.query('INSERT INTO messages (username,text,image,voice,file_data,file_name,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id',
        [msg.username, msg.text, msg.image, msg.voice, msg.file_data, msg.file_name, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id;
      io.emit('chatMessage', msg);
    } catch (e) { console.error(e); }
  });

  socket.on('deleteMessage', async (id) => {
    if (!socket.username) return;
    try {
      if (isAdmin(socket)) await pool.query('DELETE FROM messages WHERE id=$1', [id]);
      else await pool.query('DELETE FROM messages WHERE id=$1 AND username=$2', [id, socket.username]);
      io.emit('messageDeleted', id);
    } catch (e) { console.error(e); }
  });

  socket.on('typing', () => { if (socket.username) socket.broadcast.emit('userTyping', { nickname: socket.username }); });
  socket.on('stopTyping', () => { if (socket.username) socket.broadcast.emit('userStopTyping', { nickname: socket.username }); });
  socket.on('privateTyping', (toLogin) => { if (!socket.username) return; var s = findSocketByLogin(toLogin); if (s) s.emit('privateUserTyping', { from: socket.userLogin, nickname: socket.username }); });
  socket.on('privateStopTyping', (toLogin) => { if (!socket.username) return; var s = findSocketByLogin(toLogin); if (s) s.emit('privateUserStopTyping', { from: socket.userLogin }); });
  socket.on('roomTyping', (roomId) => { if (socket.username) socket.to('room_' + roomId).emit('roomUserTyping', { roomId: Number(roomId), nickname: socket.username }); });
  socket.on('roomStopTyping', (roomId) => { if (socket.username) socket.to('room_' + roomId).emit('roomUserStopTyping', { roomId: Number(roomId), nickname: socket.username }); });

  socket.on('changeNickname', async (newNick) => {
    if (!newNick || !socket.userLogin) return;
    try {
      const nickExists = await pool.query('SELECT id FROM users WHERE LOWER(nickname)=LOWER($1) AND login!=$2', [newNick, socket.userLogin]);
      if (nickExists.rows.length > 0) return socket.emit('chatError', 'Этот ник уже занят');
      await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNick, socket.userLogin]);
      socket.username = newNick;
      onlineUsers.set(socket.id, { nickname: newNick, login: socket.userLogin, ip });
      socket.emit('nicknameChanged', newNick);
      sendOnlineToAll();
    } catch (e) { console.error(e); }
  });

  socket.on('changePassword', async ({ oldPassword, newPassword }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT password FROM users WHERE login=$1', [socket.userLogin]);
      if (res.rows.length === 0) return socket.emit('passwordResult', 'Ошибка');
      const valid = await bcrypt.compare(oldPassword, res.rows[0].password);
      if (!valid) return socket.emit('passwordResult', 'Неверный старый пароль');
      const hash = await bcrypt.hash(newPassword, 10);
      await pool.query('UPDATE users SET password=$1 WHERE login=$2', [hash, socket.userLogin]);
      socket.emit('passwordResult', 'ok');
    } catch(e) { socket.emit('passwordResult', 'Ошибка'); }
  });

  socket.on('searchUser', async (query) => {
    if (!socket.userLogin || !query) return;
    try {
      const res = await pool.query('SELECT login, nickname FROM users WHERE LOWER(nickname) LIKE LOWER($1) AND login != $2 LIMIT 10', ['%' + query + '%', socket.userLogin]);
      socket.emit('searchResults', res.rows);
    } catch(e) { socket.emit('searchResults', []); }
  });

  socket.on('searchRooms', async (query) => {
    if (!query) return;
    try {
      const res = await pool.query('SELECT id, name, type FROM rooms WHERE LOWER(name) LIKE LOWER($1) LIMIT 10', ['%' + query + '%']);
      socket.emit('roomSearchResults', res.rows);
    } catch(e) { socket.emit('roomSearchResults', []); }
  });

  socket.on('getMyChats', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT DISTINCT CASE WHEN from_login=$1 THEN to_login ELSE from_login END as other_login FROM private_messages WHERE from_login=$1 OR to_login=$1', [socket.userLogin]);
      var logins = res.rows.map(r => r.other_login);
      if (logins.length === 0) return socket.emit('myChats', []);
      var users = await pool.query('SELECT login, nickname FROM users WHERE login = ANY($1)', [logins]);
      var chats = [];
      for (var u of users.rows) {
        var last = await pool.query('SELECT text, type, timestamp FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) ORDER BY timestamp DESC LIMIT 1', [socket.userLogin, u.login]);
        var unread = await pool.query('SELECT COUNT(*) as c FROM private_messages WHERE from_login=$1 AND to_login=$2 AND read=false', [u.login, socket.userLogin]);
        chats.push({ login: u.login, nickname: u.nickname, lastMsg: last.rows[0] || null, unread: parseInt(unread.rows[0].c) });
      }
      chats.sort(function(a,b) { return (b.lastMsg?b.lastMsg.timestamp:0) - (a.lastMsg?a.lastMsg.timestamp:0); });
      socket.emit('myChats', chats);
    } catch(e) { console.error(e); socket.emit('myChats', []); }
  });

  socket.on('getPrivateHistory', async (otherLogin) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) ORDER BY timestamp ASC LIMIT 200', [socket.userLogin, otherLogin]);
      await pool.query('UPDATE private_messages SET read=true WHERE from_login=$1 AND to_login=$2 AND read=false', [otherLogin, socket.userLogin]);
      socket.emit('privateHistory', { otherLogin, messages: res.rows });
    } catch(e) { console.error(e); }
  });

  socket.on('privateMessage', async (data) => {
    if (!socket.userLogin) return;
    try {
      const u = await pool.query('SELECT muted_until FROM users WHERE login=$1', [socket.userLogin]);
      if (u.rows.length > 0 && u.rows[0].muted_until > Date.now()) return socket.emit('chatError', 'Вы замучены');
    } catch(e) {}
    const msg = { 
      from_login: socket.userLogin, to_login: data.toLogin, from_nickname: socket.username, 
      text: data.text||'', image: data.image||null, voice: data.voice||null, 
      file_data: data.file_data||null, file_name: data.file_name||null,
      type: data.type||'text', timestamp: Date.now() 
    };
    try {
      const res = await pool.query('INSERT INTO private_messages (from_login,to_login,from_nickname,text,image,voice,file_data,file_name,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id',
        [msg.from_login, msg.to_login, msg.from_nickname, msg.text, msg.image, msg.voice, msg.file_data, msg.file_name, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id;
      socket.emit('newPrivateMessage', msg);
      var target = findSocketByLogin(data.toLogin);
      if (target) { target.emit('newPrivateMessage', msg); target.emit('unreadNotification', { from: socket.userLogin, nickname: socket.username }); }
    } catch(e) { console.error(e); }
  });

  socket.on('deletePrivateMessage', async (id) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM private_messages WHERE id=$1', [id]);
      if (res.rows.length === 0) return;
      var msg = res.rows[0];
      if (msg.from_login !== socket.userLogin && !isAdmin(socket)) return;
      var otherLogin = msg.from_login === socket.userLogin ? msg.to_login : msg.from_login;
      await pool.query('DELETE FROM private_messages WHERE id=$1', [id]);
      socket.emit('privateMessageDeleted', { id, otherLogin });
      var target = findSocketByLogin(otherLogin);
      if (target) target.emit('privateMessageDeleted', { id, otherLogin: socket.userLogin });
    } catch(e) { console.error(e); }
  });

  socket.on('createRoom', async ({ name, type }) => {
    if (!socket.userLogin || !name) return;
    if (type !== 'group' && type !== 'channel') type = 'group';
    try {
      const res = await pool.query('INSERT INTO rooms (name, type, owner_login, comments_enabled, timestamp) VALUES ($1,$2,$3,true,$4) RETURNING *', [name, type, socket.userLogin, Date.now()]);
      var room = res.rows[0];
      await pool.query('INSERT INTO room_members (room_id, user_login, role) VALUES ($1,$2,$3)', [room.id, socket.userLogin, 'admin']);
      socket.join('room_' + room.id);
      socket.emit('roomCreated', { id: room.id, name: room.name, type: room.type });
      await addLog('create_room', socket.username, type + ': ' + name, ip);
    } catch(e) { console.error('createRoom error:', e); socket.emit('chatError', 'Ошибка создания комнаты'); }
  });

  socket.on('getMyRooms', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query(`
        SELECT r.id, r.name, r.type, r.owner_login, r.comments_enabled, r.timestamp,
        rm.role as my_role,
        (SELECT COUNT(*)::int FROM room_members WHERE room_id=r.id) as member_count
        FROM rooms r
        JOIN room_members rm ON r.id = rm.room_id AND rm.user_login = $1
        ORDER BY r.timestamp DESC
      `, [socket.userLogin]);
      socket.emit('myRooms', res.rows);
    } catch(e) { console.error('getMyRooms error:', e); socket.emit('myRooms', []); }
  });

  socket.on('joinRoom', async (roomId) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var roomCheck = await pool.query('SELECT id FROM rooms WHERE id=$1', [roomId]);
      if (roomCheck.rows.length === 0) return socket.emit('chatError', 'Комната не найдена');
      var check = await pool.query('SELECT id FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (check.rows.length === 0) {
        await pool.query('INSERT INTO room_members (room_id, user_login, role) VALUES ($1,$2,$3)', [roomId, socket.userLogin, 'member']);
      }
      socket.join('room_' + roomId);
      socket.emit('joinedRoom', roomId);
    } catch(e) { console.error(e); }
  });

  socket.on('leaveRoom', async (roomId) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var room = await pool.query('SELECT owner_login FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length > 0 && room.rows[0].owner_login === socket.userLogin) return socket.emit('chatError', 'Владелец не может покинуть комнату. Удалите её.');
      await pool.query('DELETE FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      socket.leave('room_' + roomId);
      socket.emit('leftRoom', roomId);
    } catch(e) {}
  });

  socket.on('openRoom', async (roomId) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0) return socket.emit('chatError', 'Вы не участник этой комнаты');
      var room = await pool.query('SELECT * FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0) return socket.emit('chatError', 'Комната не найдена');
      var msgs = await pool.query('SELECT * FROM room_messages WHERE room_id=$1 ORDER BY timestamp ASC LIMIT 200', [roomId]);
      var members = await pool.query('SELECT rm.user_login, rm.role, u.nickname FROM room_members rm JOIN users u ON rm.user_login = u.login WHERE rm.room_id=$1 ORDER BY rm.role, u.nickname', [roomId]);
      socket.join('room_' + roomId);
      socket.emit('roomData', { room: room.rows[0], myRole: member.rows[0].role, messages: msgs.rows, members: members.rows });
    } catch(e) { console.error('openRoom error:', e); }
  });

  socket.on('roomMessage', async (data) => {
    if (!socket.userLogin || !data.roomId) return;
    var roomId = Number(data.roomId);
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0) return;
      var room = await pool.query('SELECT type FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0) return;
      if (room.rows[0].type === 'channel' && member.rows[0].role !== 'admin') return socket.emit('chatError', 'В канале писать может только админ');
      const msg = { 
        room_id: roomId, user_login: socket.userLogin, username: socket.username, 
        text: data.text||'', image: data.image||null, voice: data.voice||null, 
        file_data: data.file_data||null, file_name: data.file_name||null,
        type: data.type||'text', timestamp: Date.now() 
      };
      var res = await pool.query('INSERT INTO room_messages (room_id, user_login, username, text, image, voice, file_data, file_name, type, timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id',
        [msg.room_id, msg.user_login, msg.username, msg.text, msg.image, msg.voice, msg.file_data, msg.file_name, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id;
      io.to('room_' + roomId).emit('roomNewMessage', msg);
    } catch(e) { console.error(e); }
  });

  socket.on('deleteRoomMessage', async ({ roomId, msgId }) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0) return;
      if (member.rows[0].role === 'admin') await pool.query('DELETE FROM room_messages WHERE id=$1 AND room_id=$2', [msgId, roomId]);
      else await pool.query('DELETE FROM room_messages WHERE id=$1 AND room_id=$2 AND user_login=$3', [msgId, roomId, socket.userLogin]);
      io.to('room_' + roomId).emit('roomMessageDeleted', { roomId, msgId });
    } catch(e) {}
  });

  socket.on('roomAddMember', async ({ roomId, login }) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0 || member.rows[0].role !== 'admin') return socket.emit('chatError', 'Только админ может добавлять');
      var userExists = await pool.query('SELECT login FROM users WHERE login=$1', [login]);
      if (userExists.rows.length === 0) return socket.emit('chatError', 'Пользователь не найден');
      var already = await pool.query('SELECT id FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, login]);
      if (already.rows.length > 0) return socket.emit('chatError', 'Уже участник');
      await pool.query('INSERT INTO room_members (room_id, user_login, role) VALUES ($1,$2,$3)', [roomId, login, 'member']);
      var members = await pool.query('SELECT rm.user_login, rm.role, u.nickname FROM room_members rm JOIN users u ON rm.user_login = u.login WHERE rm.room_id=$1', [roomId]);
      io.to('room_' + roomId).emit('roomMembersUpdated', { roomId, members: members.rows });
      socket.emit('chatError', login + ' добавлен');
    } catch(e) { console.error(e); }
  });

  socket.on('roomRemoveMember', async ({ roomId, login }) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0 || member.rows[0].role !== 'admin') return;
      var room = await pool.query('SELECT owner_login FROM rooms WHERE id=$1', [roomId]);
      if (room.rows[0].owner_login === login) return socket.emit('chatError', 'Нельзя удалить владельца');
      await pool.query('DELETE FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, login]);
      var targetSocket = findSocketByLogin(login);
      if (targetSocket) { targetSocket.leave('room_' + roomId); targetSocket.emit('kickedFromRoom', roomId); }
      var members = await pool.query('SELECT rm.user_login, rm.role, u.nickname FROM room_members rm JOIN users u ON rm.user_login = u.login WHERE rm.room_id=$1', [roomId]);
      io.to('room_' + roomId).emit('roomMembersUpdated', { roomId, members: members.rows });
    } catch(e) {}
  });

  socket.on('roomSetRole', async ({ roomId, login, role }) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var room = await pool.query('SELECT owner_login FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0 || room.rows[0].owner_login !== socket.userLogin) return socket.emit('chatError', 'Только владелец может менять роли');
      if (role !== 'admin' && role !== 'member') return;
      await pool.query('UPDATE room_members SET role=$1 WHERE room_id=$2 AND user_login=$3', [role, roomId, login]);
      var members = await pool.query('SELECT rm.user_login, rm.role, u.nickname FROM room_members rm JOIN users u ON rm.user_login = u.login WHERE rm.room_id=$1', [roomId]);
      io.to('room_' + roomId).emit('roomMembersUpdated', { roomId, members: members.rows });
    } catch(e) {}
  });

  socket.on('roomToggleComments', async (roomId) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var room = await pool.query('SELECT * FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0 || room.rows[0].owner_login !== socket.userLogin) return;
      var newVal = !room.rows[0].comments_enabled;
      await pool.query('UPDATE rooms SET comments_enabled=$1 WHERE id=$2', [newVal, roomId]);
      io.to('room_' + roomId).emit('roomSettingsUpdated', { roomId, comments_enabled: newVal });
    } catch(e) {}
  });

  socket.on('deleteRoom', async (roomId) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      var room = await pool.query('SELECT owner_login FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0) return;
      if (room.rows[0].owner_login !== socket.userLogin && !isSuperAdmin(socket)) return;
      await pool.query('DELETE FROM room_messages WHERE room_id=$1', [roomId]);
      await pool.query('DELETE FROM room_members WHERE room_id=$1', [roomId]);
      await pool.query('DELETE FROM rooms WHERE id=$1', [roomId]);
      io.to('room_' + roomId).emit('roomDeleted', roomId);
    } catch(e) {}
  });

  socket.on('adminGetUsers', async () => {
    if (!isAdmin(socket)) return;
    try { const res = await pool.query('SELECT id,login,nickname,banned,muted_until,role FROM users ORDER BY id'); socket.emit('adminUsers', res.rows); } catch(e) {}
  });
  socket.on('adminGetStats', async () => {
    if (!isAdmin(socket)) return;
    try {
      const users = await pool.query('SELECT COUNT(*)::int as c FROM users');
      const msgs = await pool.query('SELECT COUNT(*)::int as c FROM messages');
      const pms = await pool.query('SELECT COUNT(*)::int as c FROM private_messages');
      const rooms = await pool.query('SELECT COUNT(*)::int as c FROM rooms');
      socket.emit('adminStats', { users: users.rows[0].c, messages: msgs.rows[0].c, pms: pms.rows[0].c, rooms: rooms.rows[0].c, online: onlineUsers.size });
    } catch(e) {}
  });
  socket.on('adminGetLogs', async () => {
    if (!isAdmin(socket)) return;
    try { const res = await pool.query('SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100'); socket.emit('adminLogs', res.rows); } catch(e) {}
  });
  socket.on('adminBanUser', async (login) => {
    if (!isAdmin(socket)) return;
    try { await pool.query('UPDATE users SET banned=true WHERE login=$1', [login]); var s = findSocketByLogin(login); if (s) s.emit('kicked', 'Вы заблокированы'); await addLog('ban', socket.username, 'Banned ' + login, ip); socket.emit('adminDone', 'Забанен: ' + login); } catch(e) {}
  });
  socket.on('adminUnbanUser', async (login) => {
    if (!isAdmin(socket)) return;
    try { await pool.query('UPDATE users SET banned=false WHERE login=$1', [login]); socket.emit('adminDone', 'Разбанен: ' + login); } catch(e) {}
  });
  socket.on('adminMuteUser', async ({ login, minutes }) => {
    if (!isAdmin(socket)) return;
    try { await pool.query('UPDATE users SET muted_until=$1 WHERE login=$2', [Date.now() + minutes * 60000, login]); socket.emit('adminDone', 'Замучен: ' + login); } catch(e) {}
  });
  socket.on('adminDeleteUser', async (login) => {
    if (!isSuperAdmin(socket)) return;
    if (login === ADMIN_LOGIN) return;
    try { await pool.query('DELETE FROM private_messages WHERE from_login=$1 OR to_login=$1', [login]); await pool.query('DELETE FROM room_members WHERE user_login=$1', [login]); await pool.query('DELETE FROM users WHERE login=$1', [login]); var s = findSocketByLogin(login); if (s) s.emit('kicked', 'Аккаунт удалён'); socket.emit('adminDone', 'Удалён: ' + login); } catch(e) {}
  });
  socket.on('adminDeleteAllUsers', async () => {
    if (!isSuperAdmin(socket)) return;
    try { await pool.query('DELETE FROM users WHERE login != $1', [ADMIN_LOGIN]); await pool.query('DELETE FROM messages'); await pool.query('DELETE FROM private_messages'); await pool.query('DELETE FROM room_members WHERE user_login != $1', [ADMIN_LOGIN]); for (let [sid, info] of onlineUsers) { if (info.login !== ADMIN_LOGIN) { var s = socketUsers.get(sid); if (s) s.emit('kicked', 'Все аккаунты удалены'); } } socket.emit('adminDone', 'Все удалены'); } catch(e) {}
  });
  socket.on('adminClearChat', async () => {
    if (!isAdmin(socket)) return;
    try { await pool.query('DELETE FROM messages'); io.emit('chatCleared'); socket.emit('adminDone', 'Чат очищен'); } catch(e) {}
  });
  socket.on('adminAnnounce', async (text) => {
    if (!isAdmin(socket)) return;
    const msg = { username: '⚡ Система', text, type: 'text', timestamp: Date.now() };
    try { const res = await pool.query('INSERT INTO messages (username,text,type,timestamp) VALUES ($1,$2,$3,$4) RETURNING id', [msg.username, msg.text, msg.type, msg.timestamp]); msg.id = res.rows[0].id; io.emit('chatMessage', msg); socket.emit('adminDone', 'Отправлено'); } catch(e) {}
  });
  socket.on('adminSetRole', async ({ login, role }) => {
    if (!isSuperAdmin(socket)) return;
    try { await pool.query('UPDATE users SET role=$1 WHERE login=$2', [role, login]); socket.emit('adminDone', login + ' теперь ' + role); } catch(e) {}
  });
  socket.on('adminChangeNickname', async ({ login, newNickname }) => {
    if (!isAdmin(socket)) return;
    try { await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNickname, login]); var s = findSocketByLogin(login); if (s) { s.username = newNickname; s.emit('nicknameChanged', newNickname); } for (let [sid, info] of onlineUsers) { if (info.login === login) info.nickname = newNickname; } socket.emit('adminDone', 'Ник изменён'); } catch(e) {}
  });

  socket.on('disconnect', () => {
    if (socket.username) addLog('logout', socket.username, 'Logout', ip);
    onlineUsers.delete(socket.id); socketUsers.delete(socket.id);
    sendOnlineToAll();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
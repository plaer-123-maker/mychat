const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 15e6 });

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
    role VARCHAR(20) DEFAULT 'user'
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY, username VARCHAR(100) NOT NULL,
    text TEXT, image TEXT, voice TEXT,
    type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS private_messages (
    id SERIAL PRIMARY KEY, from_login VARCHAR(50) NOT NULL,
    to_login VARCHAR(50) NOT NULL, from_nickname VARCHAR(100),
    text TEXT, image TEXT, voice TEXT,
    type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL,
    read BOOLEAN DEFAULT false
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY, action VARCHAR(50) NOT NULL,
    username VARCHAR(100), detail TEXT, ip VARCHAR(50),
    timestamp BIGINT NOT NULL
  )`);
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS muted_until BIGINT DEFAULT 0'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT \'user\''); } catch(e) {}
  try { await pool.query("UPDATE users SET role='admin' WHERE login=$1", [ADMIN_LOGIN]); } catch(e) {}
  console.log('Database ready');
}
initDB();

const onlineUsers = new Map();
const socketUsers = new Map();
const typingUsers = new Map();

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

function getOnlineList() {
  var list = [];
  for (let [sid, info] of onlineUsers) {
    list.push({ nickname: info.nickname, login: info.login });
  }
  return list;
}

function findSocketByLogin(login) {
  for (let [sid, info] of onlineUsers) {
    if (info.login === login) return socketUsers.get(sid);
  }
  return null;
}

io.on('connection', (socket) => {
  const ip = getIP(socket);

  socket.on('register', async ({ login, password, nickname }) => {
    try {
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) return socket.emit('authError', 'Этот логин уже занят');
      const hash = await bcrypt.hash(password, 10);
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      await pool.query('INSERT INTO users (login,password,nickname,banned,muted_until,role) VALUES ($1,$2,$3,false,0,$4)', [login, hash, nickname, role]);
      socket.username = nickname; socket.userLogin = login; socket.userRole = role;
      onlineUsers.set(socket.id, { nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role, login });
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
      sendOnlineToAll();
      await addLog('register', nickname, 'Зарегистрировался', ip);
    } catch (e) { console.error(e); socket.emit('authError', 'Ошибка регистрации'); }
  });

  socket.on('login', async ({ login, password }) => {
    try {
      const res = await pool.query('SELECT * FROM users WHERE login=$1', [login]);
      if (res.rows.length === 0) return socket.emit('authError', 'Неверный логин или пароль');
      const user = res.rows[0];
      if (user.banned) return socket.emit('authError', 'Ваш аккаунт заблокирован');
      const valid = await bcrypt.compare(password, user.password);
      if (!valid) return socket.emit('authError', 'Неверный логин или пароль');
      socket.username = user.nickname; socket.userLogin = login;
      socket.userRole = user.role || 'user';
      if (login === ADMIN_LOGIN) socket.userRole = 'admin';
      onlineUsers.set(socket.id, { nickname: user.nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login });
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
      sendOnlineToAll();
      await addLog('login', user.nickname, 'Вошёл в чат', ip);
    } catch (e) { console.error(e); socket.emit('authError', 'Ошибка входа'); }
  });

  // TYPING
  socket.on('typing', (chatType) => {
    if (!socket.username) return;
    if (chatType === 'general') {
      socket.broadcast.emit('userTyping', { nickname: socket.username });
    }
  });
  socket.on('stopTyping', (chatType) => {
    if (!socket.username) return;
    if (chatType === 'general') {
      socket.broadcast.emit('userStopTyping', { nickname: socket.username });
    }
  });

  // PRIVATE TYPING
  socket.on('privateTyping', (toLogin) => {
    if (!socket.username) return;
    var s = findSocketByLogin(toLogin);
    if (s) s.emit('privateUserTyping', { from: socket.userLogin, nickname: socket.username });
  });
  socket.on('privateStopTyping', (toLogin) => {
    if (!socket.username) return;
    var s = findSocketByLogin(toLogin);
    if (s) s.emit('privateUserStopTyping', { from: socket.userLogin });
  });

  socket.on('changeNickname', async (newNick) => {
    if (!newNick || !socket.userLogin) return;
    try {
      await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNick, socket.userLogin]);
      const oldNick = socket.username; socket.username = newNick;
      onlineUsers.set(socket.id, { nickname: newNick, login: socket.userLogin, ip });
      sendOnlineToAll();
      await addLog('nickname', newNick, 'Сменил ник с ' + oldNick, ip);
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
      await addLog('password', socket.username, 'Сменил пароль', ip);
      socket.emit('passwordResult', 'ok');
    } catch(e) { socket.emit('passwordResult', 'Ошибка'); }
  });

  socket.on('chatMessage', async (data) => {
    if (!socket.username) return;
    try {
      const u = await pool.query('SELECT muted_until FROM users WHERE login=$1', [socket.userLogin]);
      if (u.rows.length > 0 && u.rows[0].muted_until > Date.now()) {
        var left = Math.ceil((u.rows[0].muted_until - Date.now()) / 60000);
        return socket.emit('chatError', 'Вы замучены. Осталось ' + left + ' мин.');
      }
    } catch(e) {}
    const msg = { username: socket.username, text: data.text||'', image: data.image||null,
      voice: data.voice||null, type: data.type||'text', timestamp: Date.now() };
    try {
      const res = await pool.query(
        'INSERT INTO messages (username,text,image,voice,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
        [msg.username, msg.text, msg.image, msg.voice, msg.type, msg.timestamp]);
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

  // === PRIVATE MESSAGES ===
  socket.on('getUsers', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT login, nickname, role FROM users WHERE login != $1 ORDER BY nickname', [socket.userLogin]);
      socket.emit('usersList', res.rows);
    } catch(e) {}
  });

  socket.on('getPrivateHistory', async (otherLogin) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query(
        `SELECT * FROM private_messages WHERE 
        (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) 
        ORDER BY timestamp ASC LIMIT 200`,
        [socket.userLogin, otherLogin]);
      // Mark as read
      await pool.query('UPDATE private_messages SET read=true WHERE from_login=$1 AND to_login=$2 AND read=false',
        [otherLogin, socket.userLogin]);
      socket.emit('privateHistory', { otherLogin, messages: res.rows });
    } catch(e) { console.error(e); }
  });

  socket.on('privateMessage', async (data) => {
    if (!socket.userLogin) return;
    try {
      const u = await pool.query('SELECT muted_until FROM users WHERE login=$1', [socket.userLogin]);
      if (u.rows.length > 0 && u.rows[0].muted_until > Date.now()) {
        return socket.emit('chatError', 'Вы замучены');
      }
    } catch(e) {}
    const msg = { from_login: socket.userLogin, to_login: data.toLogin, from_nickname: socket.username,
      text: data.text||'', image: data.image||null, voice: data.voice||null,
      type: data.type||'text', timestamp: Date.now() };
    try {
      const res = await pool.query(
        'INSERT INTO private_messages (from_login,to_login,from_nickname,text,image,voice,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id',
        [msg.from_login, msg.to_login, msg.from_nickname, msg.text, msg.image, msg.voice, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id;
      socket.emit('newPrivateMessage', msg);
      var target = findSocketByLogin(data.toLogin);
      if (target) {
        target.emit('newPrivateMessage', msg);
        target.emit('unreadNotification', { from: socket.userLogin, nickname: socket.username });
      }
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

  socket.on('getUnreadCounts', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query(
        'SELECT from_login, COUNT(*) as c FROM private_messages WHERE to_login=$1 AND read=false GROUP BY from_login',
        [socket.userLogin]);
      socket.emit('unreadCounts', res.rows);
    } catch(e) {}
  });

  // === ADMIN ===
  socket.on('adminGetUsers', async () => {
    if (!isAdmin(socket)) return;
    try {
      const res = await pool.query('SELECT id,login,nickname,banned,muted_until,role FROM users ORDER BY id');
      socket.emit('adminUsers', res.rows);
    } catch(e) {}
  });
  socket.on('adminGetStats', async () => {
    if (!isAdmin(socket)) return;
    try {
      const users = await pool.query('SELECT COUNT(*) as c FROM users');
      const msgs = await pool.query('SELECT COUNT(*) as c FROM messages');
      const pms = await pool.query('SELECT COUNT(*) as c FROM private_messages');
      socket.emit('adminStats', { users: users.rows[0].c, messages: msgs.rows[0].c, pms: pms.rows[0].c, online: onlineUsers.size });
    } catch(e) {}
  });
  socket.on('adminGetLogs', async () => {
    if (!isAdmin(socket)) return;
    try {
      const res = await pool.query('SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100');
      socket.emit('adminLogs', res.rows);
    } catch(e) {}
  });
  socket.on('adminBanUser', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET banned=true WHERE login=$1', [login]);
      var s = findSocketByLogin(login); if (s) s.emit('kicked', 'Вы заблокированы');
      await addLog('ban', socket.username, 'Забанил '+login, ip);
      socket.emit('adminMsg', login+' заблокирован');
    } catch(e) {}
  });
  socket.on('adminUnbanUser', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET banned=false WHERE login=$1', [login]);
      await addLog('unban', socket.username, 'Разбанил '+login, ip);
      socket.emit('adminMsg', login+' разблокирован');
    } catch(e) {}
  });
  socket.on('adminMuteUser', async ({login, minutes}) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET muted_until=$1 WHERE login=$2', [Date.now()+minutes*60000, login]);
      await addLog('mute', socket.username, 'Замутил '+login+' на '+minutes+' мин', ip);
      socket.emit('adminMsg', login+' замучен на '+minutes+' мин');
    } catch(e) {}
  });
  socket.on('adminUnmuteUser', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET muted_until=0 WHERE login=$1', [login]);
      socket.emit('adminMsg', login+' размучен');
    } catch(e) {}
  });
  socket.on('adminDeleteUser', async (login) => {
    if (!isSuperAdmin(socket)) return;
    if (login === ADMIN_LOGIN) return;
    try {
      await pool.query('DELETE FROM users WHERE login=$1', [login]);
      await pool.query('DELETE FROM private_messages WHERE from_login=$1 OR to_login=$1', [login]);
      var s = findSocketByLogin(login); if (s) s.emit('kicked', 'Ваш аккаунт удалён');
      await addLog('delete_user', socket.username, 'Удалил '+login, ip);
      socket.emit('adminMsg', login+' удалён');
    } catch(e) {}
  });
  socket.on('adminDeleteAllUsers', async () => {
    if (!isSuperAdmin(socket)) return;
    try {
      await pool.query('DELETE FROM users WHERE login != $1', [ADMIN_LOGIN]);
      await pool.query('DELETE FROM messages');
      await pool.query('DELETE FROM private_messages');
      for (let [sid, info] of onlineUsers) {
        if (info.login !== ADMIN_LOGIN) { var s = socketUsers.get(sid); if (s) s.emit('kicked', 'Все аккаунты удалены'); }
      }
      socket.emit('adminMsg', 'Все аккаунты удалены');
    } catch(e) {}
  });
  socket.on('adminClearChat', async () => {
    if (!isAdmin(socket)) return;
    try { await pool.query('DELETE FROM messages'); io.emit('chatCleared'); socket.emit('adminMsg', 'Чат очищен'); } catch(e) {}
  });
  socket.on('adminAnnounce', async (text) => {
    if (!isAdmin(socket)) return;
    const msg = { username:'⚡ Система', text, type:'text', timestamp:Date.now() };
    try {
      const res = await pool.query('INSERT INTO messages (username,text,type,timestamp) VALUES ($1,$2,$3,$4) RETURNING id',
        [msg.username, msg.text, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id; io.emit('chatMessage', msg);
    } catch(e) {}
  });
  socket.on('adminSetRole', async ({login, role}) => {
    if (!isSuperAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET role=$1 WHERE login=$2', [role, login]);
      socket.emit('adminMsg', login+' теперь '+role);
    } catch(e) {}
  });
  socket.on('adminChangeNickname', async ({login, newNickname}) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNickname, login]);
      var s = findSocketByLogin(login);
      if (s) { s.username = newNickname; s.emit('nicknameChanged', newNickname); }
      for (let [sid, info] of onlineUsers) { if (info.login === login) info.nickname = newNickname; }
      socket.emit('adminMsg', 'Ник изменён');
    } catch(e) {}
  });
  socket.on('adminSendPrivate', async ({login, text}) => {
    if (!isAdmin(socket)) return;
    var s = findSocketByLogin(login);
    if (s) s.emit('adminPrivateMsg', { from: socket.username, text });
  });

  socket.on('disconnect', () => {
    if (socket.username) addLog('logout', socket.username, 'Вышел', ip);
    onlineUsers.delete(socket.id); socketUsers.delete(socket.id);
    sendOnlineToAll();
  });
});

function sendOnlineToAll() {
  var list = getOnlineList();
  for (let [sid, info] of onlineUsers) {
    var s = socketUsers.get(sid);
    if (!s) continue;
    if (s.userRole === 'admin' || s.userRole === 'moderator') {
      s.emit('onlineUsers', { count: list.length, users: list, isAdmin: true });
    } else {
      s.emit('onlineUsers', { count: list.length, users: [], isAdmin: false });
    }
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
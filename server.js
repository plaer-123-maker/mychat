const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

const ADMIN_LOGIN = 'pekka';

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      login VARCHAR(50) UNIQUE NOT NULL,
      password VARCHAR(200) NOT NULL,
      nickname VARCHAR(50) NOT NULL,
      banned BOOLEAN DEFAULT false,
      muted_until BIGINT DEFAULT 0,
      role VARCHAR(20) DEFAULT 'user'
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      username VARCHAR(100) NOT NULL,
      text TEXT,
      image TEXT,
      voice TEXT,
      type VARCHAR(20) DEFAULT 'text',
      timestamp BIGINT NOT NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS logs (
      id SERIAL PRIMARY KEY,
      action VARCHAR(50) NOT NULL,
      username VARCHAR(100),
      detail TEXT,
      ip VARCHAR(50),
      timestamp BIGINT NOT NULL
    )
  `);
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS muted_until BIGINT DEFAULT 0'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT \'user\''); } catch(e) {}
  try { await pool.query("UPDATE users SET role='admin' WHERE login=$1", [ADMIN_LOGIN]); } catch(e) {}
  // RESET PASSWORD - REMOVE AFTER FIRST LOGIN
  const newHash = await bcrypt.hash('123456', 10);
  await pool.query("UPDATE users SET password=$1 WHERE login='pekka'", [newHash]);
  console.log('Database ready');
}

initDB();

const onlineUsers = new Map();
const socketUsers = new Map();

function getIP(socket) {
  return socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || 'unknown';
}

async function addLog(action, username, detail, ip) {
  try {
    await pool.query('INSERT INTO logs (action, username, detail, ip, timestamp) VALUES ($1,$2,$3,$4,$5)',
      [action, username || '', detail || '', ip || '', Date.now()]);
  } catch(e) {}
}

function isAdmin(socket) {
  return socket.userLogin === ADMIN_LOGIN || socket.userRole === 'admin' || socket.userRole === 'moderator';
}

function isSuperAdmin(socket) {
  return socket.userLogin === ADMIN_LOGIN;
}

io.on('connection', (socket) => {
  const ip = getIP(socket);
  console.log('User connected from', ip);

  socket.on('register', async ({ login, password, nickname }) => {
    try {
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) {
        return socket.emit('authError', 'Этот логин уже занят');
      }
      const hash = await bcrypt.hash(password, 10);
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      await pool.query('INSERT INTO users (login, password, nickname, banned, muted_until, role) VALUES ($1,$2,$3,false,0,$4)', [login, hash, nickname, role]);
      socket.username = nickname;
      socket.userLogin = login;
      socket.userRole = role;
      onlineUsers.set(socket.id, { nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role });
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
      io.emit('onlineUsers', getOnlineList());
      await addLog('register', nickname, 'Зарегистрировался', ip);
    } catch (e) {
      console.error(e);
      socket.emit('authError', 'Ошибка регистрации');
    }
  });

  socket.on('login', async ({ login, password }) => {
    try {
      const res = await pool.query('SELECT * FROM users WHERE login=$1', [login]);
      if (res.rows.length === 0) {
        return socket.emit('authError', 'Неверный логин или пароль');
      }
      const user = res.rows[0];
      if (user.banned) {
        return socket.emit('authError', 'Ваш аккаунт заблокирован');
      }
      const valid = await bcrypt.compare(password, user.password);
      if (!valid) {
        return socket.emit('authError', 'Неверный логин или пароль');
      }
      socket.username = user.nickname;
      socket.userLogin = login;
      socket.userRole = user.role || 'user';
      if (login === ADMIN_LOGIN) socket.userRole = 'admin';
      onlineUsers.set(socket.id, { nickname: user.nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole });
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
      io.emit('onlineUsers', getOnlineList());
      await addLog('login', user.nickname, 'Вошёл в чат', ip);
    } catch (e) {
      console.error(e);
      socket.emit('authError', 'Ошибка входа');
    }
  });

  socket.on('changeNickname', async (newNick) => {
    if (!newNick || !socket.userLogin) return;
    try {
      await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNick, socket.userLogin]);
      const oldNick = socket.username;
      socket.username = newNick;
      onlineUsers.set(socket.id, { nickname: newNick, login: socket.userLogin, ip });
      io.emit('onlineUsers', getOnlineList());
      await addLog('nickname', newNick, 'Сменил ник с ' + oldNick, ip);
    } catch (e) {
      console.error(e);
    }
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

    const msg = {
      username: socket.username,
      text: data.text || '',
      image: data.image || null,
      voice: data.voice || null,
      type: data.type || 'text',
      timestamp: Date.now()
    };
    try {
      const res = await pool.query(
        'INSERT INTO messages (username, text, image, voice, type, timestamp) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
        [msg.username, msg.text, msg.image, msg.voice, msg.type, msg.timestamp]
      );
      msg.id = res.rows[0].id;
      io.emit('chatMessage', msg);
    } catch (e) {
      console.error(e);
    }
  });

  socket.on('deleteMessage', async (id) => {
    if (!socket.username) return;
    try {
      if (isAdmin(socket)) {
        await pool.query('DELETE FROM messages WHERE id=$1', [id]);
      } else {
        await pool.query('DELETE FROM messages WHERE id=$1 AND username=$2', [id, socket.username]);
      }
      io.emit('messageDeleted', id);
    } catch (e) {
      console.error(e);
    }
  });

  // === ADMIN ===
  socket.on('adminGetUsers', async () => {
    if (!isAdmin(socket)) return;
    try {
      const res = await pool.query('SELECT id, login, nickname, banned, muted_until, role FROM users ORDER BY id');
      socket.emit('adminUsers', res.rows);
    } catch(e) { console.error(e); }
  });

  socket.on('adminGetStats', async () => {
    if (!isAdmin(socket)) return;
    try {
      const users = await pool.query('SELECT COUNT(*) as c FROM users');
      const msgs = await pool.query('SELECT COUNT(*) as c FROM messages');
      const online = onlineUsers.size;
      socket.emit('adminStats', { users: users.rows[0].c, messages: msgs.rows[0].c, online: online });
    } catch(e) { console.error(e); }
  });

  socket.on('adminGetLogs', async () => {
    if (!isAdmin(socket)) return;
    try {
      const res = await pool.query('SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100');
      socket.emit('adminLogs', res.rows);
    } catch(e) { console.error(e); }
  });

  socket.on('adminBanUser', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET banned=true WHERE login=$1', [login]);
      for (let [sid, info] of onlineUsers) {
        if (info.login === login) {
          var s = socketUsers.get(sid);
          if (s) s.emit('kicked', 'Вы заблокированы');
        }
      }
      await addLog('ban', socket.username, 'Забанил ' + login, ip);
      socket.emit('adminMsg', 'Пользователь ' + login + ' заблокирован');
    } catch(e) { console.error(e); }
  });

  socket.on('adminUnbanUser', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET banned=false WHERE login=$1', [login]);
      await addLog('unban', socket.username, 'Разбанил ' + login, ip);
      socket.emit('adminMsg', 'Пользователь ' + login + ' разблокирован');
    } catch(e) { console.error(e); }
  });

  socket.on('adminMuteUser', async ({ login, minutes }) => {
    if (!isAdmin(socket)) return;
    try {
      var until = Date.now() + minutes * 60000;
      await pool.query('UPDATE users SET muted_until=$1 WHERE login=$2', [until, login]);
      await addLog('mute', socket.username, 'Замутил ' + login + ' на ' + minutes + ' мин', ip);
      socket.emit('adminMsg', login + ' замучен на ' + minutes + ' минут');
    } catch(e) { console.error(e); }
  });

  socket.on('adminUnmuteUser', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET muted_until=0 WHERE login=$1', [login]);
      await addLog('unmute', socket.username, 'Размутил ' + login, ip);
      socket.emit('adminMsg', login + ' размучен');
    } catch(e) { console.error(e); }
  });

  socket.on('adminDeleteUser', async (login) => {
    if (!isSuperAdmin(socket)) return;
    if (login === ADMIN_LOGIN) return;
    try {
      await pool.query('DELETE FROM users WHERE login=$1', [login]);
      for (let [sid, info] of onlineUsers) {
        if (info.login === login) {
          var s = socketUsers.get(sid);
          if (s) s.emit('kicked', 'Ваш аккаунт удалён');
        }
      }
      await addLog('delete_user', socket.username, 'Удалил аккаунт ' + login, ip);
      socket.emit('adminMsg', 'Аккаунт ' + login + ' удалён');
    } catch(e) { console.error(e); }
  });

  socket.on('adminDeleteAllUsers', async () => {
    if (!isSuperAdmin(socket)) return;
    try {
      await pool.query('DELETE FROM users WHERE login != $1', [ADMIN_LOGIN]);
      await pool.query('DELETE FROM messages');
      for (let [sid, info] of onlineUsers) {
        if (info.login !== ADMIN_LOGIN) {
          var s = socketUsers.get(sid);
          if (s) s.emit('kicked', 'Все аккаунты удалены');
        }
      }
      await addLog('delete_all', socket.username, 'Удалил все аккаунты', ip);
      socket.emit('adminMsg', 'Все аккаунты удалены');
    } catch(e) { console.error(e); }
  });

  socket.on('adminClearChat', async () => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('DELETE FROM messages');
      io.emit('chatCleared');
      await addLog('clear_chat', socket.username, 'Очистил чат', ip);
      socket.emit('adminMsg', 'Чат очищен');
    } catch(e) { console.error(e); }
  });

  socket.on('adminAnnounce', async (text) => {
    if (!isAdmin(socket)) return;
    const msg = {
      username: '⚡ Система',
      text: text,
      type: 'text',
      timestamp: Date.now()
    };
    try {
      const res = await pool.query(
        'INSERT INTO messages (username, text, type, timestamp) VALUES ($1,$2,$3,$4) RETURNING id',
        [msg.username, msg.text, msg.type, msg.timestamp]
      );
      msg.id = res.rows[0].id;
      io.emit('chatMessage', msg);
    } catch(e) { console.error(e); }
  });

  socket.on('adminSetRole', async ({ login, role }) => {
    if (!isSuperAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET role=$1 WHERE login=$2', [role, login]);
      await addLog('set_role', socket.username, 'Назначил ' + login + ' роль ' + role, ip);
      socket.emit('adminMsg', login + ' теперь ' + role);
    } catch(e) { console.error(e); }
  });

  socket.on('adminChangeNickname', async ({ login, newNickname }) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNickname, login]);
      for (let [sid, info] of onlineUsers) {
        if (info.login === login) {
          info.nickname = newNickname;
          var s = socketUsers.get(sid);
          if (s) { s.username = newNickname; s.emit('nicknameChanged', newNickname); }
        }
      }
      await addLog('change_nick', socket.username, 'Сменил ник ' + login + ' на ' + newNickname, ip);
      socket.emit('adminMsg', 'Ник изменён на ' + newNickname);
    } catch(e) { console.error(e); }
  });

  socket.on('adminSendPrivate', async ({ login, text }) => {
    if (!isAdmin(socket)) return;
    for (let [sid, info] of onlineUsers) {
      if (info.login === login) {
        var s = socketUsers.get(sid);
        if (s) s.emit('privateMessage', { from: socket.username, text: text });
      }
    }
  });

  socket.on('disconnect', () => {
    if (socket.username) addLog('logout', socket.username, 'Вышел из чата', ip);
    onlineUsers.delete(socket.id);
    socketUsers.delete(socket.id);
    io.emit('onlineUsers', getOnlineList());
  });
});

function getOnlineList() {
  var list = [];
  for (let [sid, info] of onlineUsers) {
    list.push({ nickname: info.nickname, login: info.login });
  }
  return list;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
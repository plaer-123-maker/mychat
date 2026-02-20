const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 20e6 });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Хэш пароля (простой SHA256)
function hashPassword(password) {
  return crypto.createHash('sha256').update(password + 'mychat_salt_2024').digest('hex');
}

async function initDB() {
  // Таблица пользователей
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      login VARCHAR(30) UNIQUE NOT NULL,
      password_hash VARCHAR(64) NOT NULL,
      nickname VARCHAR(30) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);

  // Таблица сообщений
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id BIGSERIAL PRIMARY KEY,
      type VARCHAR(10) DEFAULT 'text',
      user_id INTEGER REFERENCES users(id),
      username VARCHAR(30),
      content TEXT,
      duration INTEGER DEFAULT 0,
      time VARCHAR(10),
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);

  console.log('✅ База данных готова');
}

app.use(express.static(path.join(__dirname, 'public')));

const onlineUsers = {}; // socket.id -> { userId, login, nickname }

io.on('connection', (socket) => {

  // РЕГИСТРАЦИЯ
  socket.on('register', async ({ login, password, nickname }) => {
    login = login.trim().toLowerCase();
    nickname = nickname.trim();

    if (login.length < 3) return socket.emit('auth_error', 'Логин минимум 3 символа');
    if (password.length < 4) return socket.emit('auth_error', 'Пароль минимум 4 символа');
    if (nickname.length < 1) return socket.emit('auth_error', 'Введи ник');

    try {
      const result = await pool.query(
        'INSERT INTO users (login, password_hash, nickname) VALUES ($1, $2, $3) RETURNING id, login, nickname',
        [login, hashPassword(password), nickname]
      );
      const user = result.rows[0];
      socket.userId = user.id;
      socket.login = user.login;
      socket.nickname = user.nickname;
      onlineUsers[socket.id] = { userId: user.id, login: user.login, nickname: user.nickname };

      socket.emit('auth_success', { login: user.login, nickname: user.nickname });
      await sendHistory(socket);
      broadcastUsers();
    } catch (e) {
      if (e.code === '23505') socket.emit('auth_error', 'Этот логин уже занят!');
      else socket.emit('auth_error', 'Ошибка регистрации');
    }
  });

  // ВХОД
  socket.on('login', async ({ login, password }) => {
    login = login.trim().toLowerCase();
    try {
      const result = await pool.query(
        'SELECT id, login, nickname FROM users WHERE login = $1 AND password_hash = $2',
        [login, hashPassword(password)]
      );
      if (result.rows.length === 0) return socket.emit('auth_error', 'Неверный логин или пароль');

      const user = result.rows[0];
      socket.userId = user.id;
      socket.login = user.login;
      socket.nickname = user.nickname;
      onlineUsers[socket.id] = { userId: user.id, login: user.login, nickname: user.nickname };

      socket.emit('auth_success', { login: user.login, nickname: user.nickname });
      await sendHistory(socket);
      broadcastUsers();
    } catch (e) {
      socket.emit('auth_error', 'Ошибка входа');
    }
  });

  // СМЕНА НИКА
  socket.on('change_nickname', async (newNick) => {
    if (!socket.userId) return;
    newNick = newNick.trim();
    if (newNick.length < 1) return socket.emit('nick_error', 'Ник не может быть пустым');
    if (newNick.length > 30) return socket.emit('nick_error', 'Ник слишком длинный');

    await pool.query('UPDATE users SET nickname = $1 WHERE id = $2', [newNick, socket.userId]);
    socket.nickname = newNick;
    onlineUsers[socket.id].nickname = newNick;

    socket.emit('nick_changed', newNick);
    broadcastUsers();
    io.emit('system', `${socket.nickname} сменил ник`);
  });

  // ОТПРАВКА СООБЩЕНИЙ
  socket.on('message', async (text) => {
    if (!socket.userId) return;
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const result = await pool.query(
      'INSERT INTO messages (type, user_id, username, content, time) VALUES ($1,$2,$3,$4,$5) RETURNING id',
      ['text', socket.userId, socket.nickname, text, time]
    );
    io.emit('message', { id: result.rows[0].id, type: 'text', username: socket.nickname, userId: socket.userId, text, time });
  });

  socket.on('image', async (data) => {
    if (!socket.userId) return;
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const result = await pool.query(
      'INSERT INTO messages (type, user_id, username, content, time) VALUES ($1,$2,$3,$4,$5) RETURNING id',
      ['image', socket.userId, socket.nickname, data.imageData, time]
    );
    io.emit('message', { id: result.rows[0].id, type: 'image', username: socket.nickname, userId: socket.userId, imageData: data.imageData, time });
  });

  socket.on('voice', async (data) => {
    if (!socket.userId) return;
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const result = await pool.query(
      'INSERT INTO messages (type, user_id, username, content, duration, time) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
      ['voice', socket.userId, socket.nickname, data.audioData, data.duration || 0, time]
    );
    io.emit('message', { id: result.rows[0].id, type: 'voice', username: socket.nickname, userId: socket.userId, audioData: data.audioData, duration: data.duration, time });
  });

  // УДАЛЕНИЕ
  socket.on('delete_message', async (msgId) => {
    if (!socket.userId) return;
    const result = await pool.query('SELECT user_id FROM messages WHERE id = $1', [msgId]);
    if (result.rows.length === 0) return;
    if (result.rows[0].user_id !== socket.userId) return;
    await pool.query('DELETE FROM messages WHERE id = $1', [msgId]);
    io.emit('message_deleted', msgId);
  });

  socket.on('typing', () => { if (socket.nickname) socket.broadcast.emit('typing', socket.nickname); });
  socket.on('stop_typing', () => { if (socket.nickname) socket.broadcast.emit('stop_typing', socket.nickname); });

  socket.on('disconnect', () => {
    if (socket.nickname) {
      delete onlineUsers[socket.id];
      broadcastUsers();
    }
  });

  async function sendHistory(socket) {
    const result = await pool.query('SELECT * FROM messages ORDER BY created_at DESC LIMIT 100');
    const history = result.rows.reverse().map(row => ({
      id: row.id, type: row.type, username: row.username, userId: row.user_id,
      text: row.type === 'text' ? row.content : undefined,
      imageData: row.type === 'image' ? row.content : undefined,
      audioData: row.type === 'voice' ? row.content : undefined,
      duration: row.duration, time: row.time
    }));
    socket.emit('history', history);
  }

  function broadcastUsers() {
    const users = Object.values(onlineUsers).map(u => u.nickname);
    io.emit('users_update', { users, count: users.length });
  }
});

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  server.listen(PORT, () => console.log('✅ Сервер запущен на порту ' + PORT));
});

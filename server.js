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

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      login VARCHAR(50) UNIQUE NOT NULL,
      password VARCHAR(200) NOT NULL,
      nickname VARCHAR(50) NOT NULL
    )
  `);
  await pool.query(`
    CREATE TABLE messages (
      id SERIAL PRIMARY KEY,
      username VARCHAR(100) NOT NULL,
      text TEXT,
      image TEXT,
      voice TEXT,
      type VARCHAR(20) DEFAULT 'text',
      timestamp BIGINT NOT NULL
    )
  `);
  console.log('Database ready');
}

initDB();

const onlineUsers = new Map();

io.on('connection', (socket) => {
  console.log('User connected');

  socket.on('register', async ({ login, password, nickname }) => {
    try {
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) {
        return socket.emit('authError', 'Этот логин уже занят');
      }
      const hash = await bcrypt.hash(password, 10);
      await pool.query('INSERT INTO users (login, password, nickname) VALUES ($1,$2,$3)', [login, hash, nickname]);
      socket.username = nickname;
      socket.userLogin = login;
      onlineUsers.set(socket.id, nickname);
      socket.emit('authSuccess', { nickname });
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
      io.emit('onlineUsers', Array.from(onlineUsers.values()));
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
      const valid = await bcrypt.compare(password, user.password);
      if (!valid) {
        return socket.emit('authError', 'Неверный логин или пароль');
      }
      socket.username = user.nickname;
      socket.userLogin = login;
      onlineUsers.set(socket.id, user.nickname);
      socket.emit('authSuccess', { nickname: user.nickname });
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows);
      io.emit('onlineUsers', Array.from(onlineUsers.values()));
    } catch (e) {
      console.error(e);
      socket.emit('authError', 'Ошибка входа');
    }
  });

  socket.on('changeNickname', async (newNick) => {
    if (!newNick || !socket.userLogin) return;
    try {
      await pool.query('UPDATE users SET nickname=$1 WHERE login=$2', [newNick, socket.userLogin]);
      socket.username = newNick;
      onlineUsers.set(socket.id, newNick);
      io.emit('onlineUsers', Array.from(onlineUsers.values()));
    } catch (e) {
      console.error(e);
    }
  });

  socket.on('chatMessage', async (data) => {
    if (!socket.username) return;
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
      await pool.query('DELETE FROM messages WHERE id=$1 AND username=$2', [id, socket.username]);
      io.emit('messageDeleted', id);
    } catch (e) {
      console.error(e);
    }
  });

  socket.on('disconnect', () => {
    onlineUsers.delete(socket.id);
    io.emit('onlineUsers', Array.from(onlineUsers.values()));
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
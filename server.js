const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 20e6
});

// Подключение к PostgreSQL (Railway автоматически даёт DATABASE_URL)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Создаём таблицу если её нет
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id BIGSERIAL PRIMARY KEY,
      type VARCHAR(10) DEFAULT 'text',
      username VARCHAR(50),
      content TEXT,
      duration INTEGER DEFAULT 0,
      time VARCHAR(10),
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);
  console.log('✅ База данных готова');
}

app.use(express.static(path.join(__dirname, 'public')));

const users = {};

io.on('connection', async (socket) => {

  socket.on('join', async (username) => {
    users[socket.id] = username;
    socket.username = username;

    // Загружаем последние 100 сообщений из БД
    const result = await pool.query(
      'SELECT * FROM messages ORDER BY created_at DESC LIMIT 100'
    );
    const history = result.rows.reverse().map(row => ({
      id: row.id,
      type: row.type,
      username: row.username,
      text: row.type === 'text' ? row.content : undefined,
      imageData: row.type === 'image' ? row.content : undefined,
      audioData: row.type === 'voice' ? row.content : undefined,
      duration: row.duration,
      time: row.time
    }));

    socket.emit('history', history);

    io.emit('user_joined', {
      username,
      users: Object.values(users),
      count: Object.keys(users).length
    });
  });

  // Текст
  socket.on('message', async (text) => {
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const result = await pool.query(
      'INSERT INTO messages (type, username, content, time) VALUES ($1, $2, $3, $4) RETURNING id',
      ['text', socket.username, text, time]
    );
    const msg = { id: result.rows[0].id, type: 'text', username: socket.username, text, time };
    io.emit('message', msg);
  });

  // Фото
  socket.on('image', async (data) => {
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const result = await pool.query(
      'INSERT INTO messages (type, username, content, time) VALUES ($1, $2, $3, $4) RETURNING id',
      ['image', socket.username, data.imageData, time]
    );
    const msg = { id: result.rows[0].id, type: 'image', username: socket.username, imageData: data.imageData, time };
    io.emit('message', msg);
  });

  // Голосовое
  socket.on('voice', async (data) => {
    const time = new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const result = await pool.query(
      'INSERT INTO messages (type, username, content, duration, time) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      ['voice', socket.username, data.audioData, data.duration || 0, time]
    );
    const msg = { id: result.rows[0].id, type: 'voice', username: socket.username, audioData: data.audioData, duration: data.duration, time };
    io.emit('message', msg);
  });

  // Удаление сообщения
  socket.on('delete_message', async (msgId) => {
    // Получаем сообщение — удалять может только автор
    const result = await pool.query('SELECT username FROM messages WHERE id = $1', [msgId]);
    if (result.rows.length === 0) return;
    if (result.rows[0].username !== socket.username) return; // чужое не удалить

    await pool.query('DELETE FROM messages WHERE id = $1', [msgId]);
    io.emit('message_deleted', msgId); // уведомляем всех
  });

  socket.on('typing', () => { socket.broadcast.emit('typing', socket.username); });
  socket.on('stop_typing', () => { socket.broadcast.emit('stop_typing', socket.username); });

  socket.on('disconnect', () => {
    if (socket.username) {
      delete users[socket.id];
      io.emit('user_left', {
        username: socket.username,
        users: Object.values(users),
        count: Object.keys(users).length
      });
    }
  });
});

const PORT = process.env.PORT || 3000;

initDB().then(() => {
  server.listen(PORT, () => {
    console.log('\n✅ Чат запущен!');
    console.log('🌐 http://localhost:' + PORT + '\n');
  });
});

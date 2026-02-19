const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// Храним пользователей онлайн
const users = {};
// Храним историю сообщений (последние 100)
const messages = [];

io.on('connection', (socket) => {
  console.log('Новый пользователь подключился:', socket.id);

  // Пользователь входит с именем
  socket.on('join', (username) => {
    users[socket.id] = username;
    socket.username = username;

    // Отправляем историю сообщений новому пользователю
    socket.emit('history', messages);

    // Уведомляем всех о новом пользователе
    io.emit('user_joined', {
      username,
      users: Object.values(users),
      count: Object.keys(users).length
    });

    console.log(`${username} вошёл в чат`);
  });

  // Получаем сообщение и рассылаем всем
  socket.on('message', (text) => {
    const msg = {
      id: Date.now(),
      username: socket.username || 'Аноним',
      text,
      time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
    };

    messages.push(msg);
    if (messages.length > 100) messages.shift(); // храним только 100 сообщений

    io.emit('message', msg);
  });

  // Пользователь печатает
  socket.on('typing', () => {
    socket.broadcast.emit('typing', socket.username);
  });

  socket.on('stop_typing', () => {
    socket.broadcast.emit('stop_typing', socket.username);
  });

  // Пользователь отключился
  socket.on('disconnect', () => {
    if (socket.username) {
      delete users[socket.id];
      io.emit('user_left', {
        username: socket.username,
        users: Object.values(users),
        count: Object.keys(users).length
      });
      console.log(`${socket.username} вышел из чата`);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n✅ Чат запущен!`);
  console.log(`🌐 Открой в браузере: http://localhost:${PORT}`);
  console.log(`📱 Другие в твоей сети могут зайти по твоему IP\n`);
});

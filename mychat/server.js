const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 5e6 // 5MB для фото
});

app.use(express.static(path.join(__dirname, 'public')));

const users = {};
const messages = [];

io.on('connection', (socket) => {
  console.log('Новый пользователь:', socket.id);

  socket.on('join', (username) => {
    users[socket.id] = username;
    socket.username = username;
    socket.emit('history', messages);
    io.emit('user_joined', {
      username,
      users: Object.values(users),
      count: Object.keys(users).length
    });
  });

  // Текстовое сообщение
  socket.on('message', (text) => {
    const msg = {
      id: Date.now(),
      username: socket.username || 'Аноним',
      text,
      type: 'text',
      time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
    };
    messages.push(msg);
    if (messages.length > 100) messages.shift();
    io.emit('message', msg);
  });

  // Фото
  socket.on('image', (data) => {
    const msg = {
      id: Date.now(),
      username: socket.username || 'Аноним',
      image: data,
      type: 'image',
      time: new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
    };
    messages.push(msg);
    if (messages.length > 100) messages.shift();
    io.emit('message', msg);
  });

  socket.on('typing', () => socket.broadcast.emit('typing', socket.username));
  socket.on('stop_typing', () => socket.broadcast.emit('stop_typing', socket.username));

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
server.listen(PORT, () => {
  console.log(`✅ Чат запущен: http://localhost:${PORT}`);
});
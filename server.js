const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const bcrypt = require('bcrypt'); // Для безопасного хранения паролей
const socketIo = require('socket.io');
const server = require('http').Server(app);
const io = socketIo(server);

app.use(bodyParser.json());

let users = {}; // В этой структуре будут храниться пользователи (логин и пароль)

// Endpoint для регистрации
app.post('/register', async (req, res) => {
  const { username, password } = req.body;

  // Проверка на существование пользователя
  if (users[username]) {
    return res.status(400).send('Пользователь с таким логином уже существует');
  }

  // Хэшируем пароль
  const hashedPassword = await bcrypt.hash(password, 10);
  users[username] = hashedPassword; // Сохраняем пользователя

  res.status(201).send('Регистрация успешна');
});

// Endpoint для входа
app.post('/login', async (req, res) => {
  const { username, password } = req.body;

  if (!users[username]) {
    return res.status(400).send('Пользователь не найден');
  }

  // Проверка пароля
  const isMatch = await bcrypt.compare(password, users[username]);
  if (!isMatch) {
    return res.status(400).send('Неверный пароль');
  }

  res.status(200).send('Вход успешен');
});

// WebSocket для общения в чате
io.on('connection', (socket) => {
  console.log('A user connected');

  // Пример сообщения, отправляемого в чат
  socket.on('message', (msg) => {
    io.emit('message', msg);
  });

  socket.on('disconnect', () => {
    console.log('User disconnected');
  });
});

// Запуск сервера на порту 3000
server.listen(3000, () => {
  console.log('Server is running on port 3000');
});

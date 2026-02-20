const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 20e6 }); // Увеличил лимит до 20МБ

app.use(express.static('public'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

const ADMIN_LOGIN = 'pekka';

async function initDB() {
  // Пользователи
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY, login VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(200) NOT NULL, nickname VARCHAR(50) NOT NULL,
    avatar TEXT, banned BOOLEAN DEFAULT false, muted_until BIGINT DEFAULT 0,
    role VARCHAR(20) DEFAULT 'user'
  )`);
  // Глобальные сообщения
  await pool.query(`CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY, username VARCHAR(100) NOT NULL,
    text TEXT, image TEXT, voice TEXT, video TEXT,
    reply_to INT, type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL
  )`);
  // Личные сообщения
  await pool.query(`CREATE TABLE IF NOT EXISTS private_messages (
    id SERIAL PRIMARY KEY, from_login VARCHAR(50) NOT NULL,
    to_login VARCHAR(50) NOT NULL, from_nickname VARCHAR(100),
    text TEXT, image TEXT, voice TEXT, video TEXT,
    reply_to INT, type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL,
    read BOOLEAN DEFAULT false
  )`);
  // Комнаты (Группы и Каналы)
  await pool.query(`CREATE TABLE IF NOT EXISTS rooms (
    id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL, owner_login VARCHAR(50) NOT NULL,
    avatar TEXT, settings JSONB DEFAULT '{}'
  )`);
  // Участники комнат
  await pool.query(`CREATE TABLE IF NOT EXISTS room_members (
    room_id INT NOT NULL, user_login VARCHAR(50) NOT NULL,
    role VARCHAR(20) DEFAULT 'member',
    PRIMARY KEY (room_id, user_login)
  )`);
  // Сообщения комнат
  await pool.query(`CREATE TABLE IF NOT EXISTS room_messages (
    id SERIAL PRIMARY KEY, room_id INT NOT NULL,
    username VARCHAR(100) NOT NULL, user_login VARCHAR(50) NOT NULL,
    text TEXT, image TEXT, voice TEXT, video TEXT,
    reply_to INT, type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL
  )`);
  // Логи
  await pool.query(`CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY, action VARCHAR(50) NOT NULL,
    username VARCHAR(100), detail TEXT, ip VARCHAR(50), timestamp BIGINT NOT NULL
  )`);

  // Обновления таблиц (если старые версии)
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT'); } catch(e){}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS video TEXT'); } catch(e){}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to INT'); } catch(e){}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS video TEXT'); } catch(e){}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS reply_to INT'); } catch(e){}
  try { await pool.query("UPDATE users SET role='admin' WHERE login=$1", [ADMIN_LOGIN]); } catch(e) {}
  
  console.log('Database ready');
}
initDB();

const onlineUsers = new Map(); // socket.id -> {nickname, login, avatar}
const socketUsers = new Map();

function getIP(socket) { return socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || 'unknown'; }
async function addLog(action, username, detail, ip) {
  try { await pool.query('INSERT INTO logs (action,username,detail,ip,timestamp) VALUES ($1,$2,$3,$4,$5)', [action, username||'', detail||'', ip||'', Date.now()]); } catch(e) {}
}
function isAdmin(socket) { return socket.userLogin === ADMIN_LOGIN || socket.userRole === 'admin' || socket.userRole === 'moderator'; }
function isSuperAdmin(socket) { return socket.userLogin === ADMIN_LOGIN; }

io.on('connection', (socket) => {
  const ip = getIP(socket);

  socket.on('register', async ({ login, password, nickname }) => {
    try {
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) return socket.emit('authError', 'Логин занят');
      const hash = await bcrypt.hash(password, 10);
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      await pool.query('INSERT INTO users (login,password,nickname,role) VALUES ($1,$2,$3,$4)', [login, hash, nickname, role]);
      socket.username = nickname; socket.userLogin = login; socket.userRole = role; socket.userAvatar = null;
      onlineUsers.set(socket.id, { nickname, login, avatar: null, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role, login, avatar: null });
      sendOnlineToAll();
    } catch (e) { socket.emit('authError', 'Ошибка регистрации'); }
  });

  socket.on('login', async ({ login, password }) => {
    try {
      const res = await pool.query('SELECT * FROM users WHERE login=$1', [login]);
      if (res.rows.length === 0) return socket.emit('authError', 'Неверно');
      const user = res.rows[0];
      if (user.banned) return socket.emit('authError', 'Бан');
      const valid = await bcrypt.compare(password, user.password);
      if (!valid) return socket.emit('authError', 'Неверно');
      socket.username = user.nickname; socket.userLogin = login;
      socket.userRole = user.role || 'user'; socket.userAvatar = user.avatar;
      if (login === ADMIN_LOGIN) socket.userRole = 'admin';
      onlineUsers.set(socket.id, { nickname: user.nickname, login, avatar: user.avatar, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login, avatar: user.avatar });
      sendOnlineToAll();
    } catch (e) { socket.emit('authError', 'Ошибка входа'); }
  });

  // AVATAR UPDATE
  socket.on('updateAvatar', async (base64) => {
    if (!socket.userLogin) return;
    try {
      await pool.query('UPDATE users SET avatar=$1 WHERE login=$2', [base64, socket.userLogin]);
      socket.userAvatar = base64;
      var info = onlineUsers.get(socket.id);
      if(info) { info.avatar = base64; onlineUsers.set(socket.id, info); }
      socket.emit('avatarUpdated', base64);
      sendOnlineToAll(); // Update avatars in lists
    } catch(e){}
  });

  // === ROOMS (GROUPS/CHANNELS) ===
  socket.on('createRoom', async ({ name, type, avatar }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query(
        'INSERT INTO rooms (name, type, owner_login, avatar) VALUES ($1,$2,$3,$4) RETURNING id',
        [name, type, socket.userLogin, avatar]
      );
      const roomId = res.rows[0].id;
      await pool.query('INSERT INTO room_members (room_id, user_login, role) VALUES ($1,$2,$3)',
        [roomId, socket.userLogin, 'admin']);
      socket.emit('roomCreated', { id: roomId, name, type });
      socket.emit('myRoomsList', await getMyRooms(socket.userLogin));
    } catch(e){ console.error(e); }
  });

  socket.on('getMyRooms', async () => {
    if (!socket.userLogin) return;
    socket.emit('myRoomsList', await getMyRooms(socket.userLogin));
  });

  async function getMyRooms(login) {
    try {
      const res = await pool.query(`
        SELECT r.id, r.name, r.type, r.avatar, rm.role,
        (SELECT COUNT(*) FROM room_messages WHERE room_id=r.id) as msg_count
        FROM rooms r
        JOIN room_members rm ON r.id = rm.room_id
        WHERE rm.user_login = $1
      `, [login]);
      return res.rows;
    } catch(e){ return []; }
  }

  socket.on('joinRoom', async (roomId) => {
    if (!socket.userLogin) return;
    socket.join('room_' + roomId);
    // Load history
    const msgs = await pool.query('SELECT * FROM room_messages WHERE room_id=$1 ORDER BY timestamp ASC LIMIT 100', [roomId]);
    socket.emit('roomHistory', { roomId, messages: msgs.rows });
    // Check permissions
    const member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
    socket.emit('roomPermissions', { roomId, role: member.rows.length>0 ? member.rows[0].role : null });
  });

  socket.on('searchPublicRooms', async (query) => {
    if (!query) return;
    try {
      const res = await pool.query('SELECT id, name, type, avatar FROM rooms WHERE LOWER(name) LIKE LOWER($1) LIMIT 10', ['%'+query+'%']);
      socket.emit('roomSearchResults', res.rows);
    } catch(e){}
  });

  socket.on('enterRoom', async (roomId) => {
    if(!socket.userLogin) return;
    // Check if member, if not join as member
    try {
      const check = await pool.query('SELECT * FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if(check.rows.length === 0) {
        await pool.query('INSERT INTO room_members (room_id, user_login, role) VALUES ($1,$2,$3)', [roomId, socket.userLogin, 'member']);
      }
      socket.emit('myRoomsList', await getMyRooms(socket.userLogin));
      socket.emit('joinedRoom', roomId);
    } catch(e){}
  });

  socket.on('roomMessage', async (data) => {
    if (!socket.userLogin) return;
    // Check perms
    const mem = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [data.roomId, socket.userLogin]);
    if (mem.rows.length === 0) return;
    const role = mem.rows[0].role;
    const room = await pool.query('SELECT type FROM rooms WHERE id=$1', [data.roomId]);
    
    // In Channels only admin can post
    if (room.rows[0].type === 'channel' && role !== 'admin') {
      return socket.emit('chatError', 'В этом канале писать может только администратор');
    }

    const msg = {
      room_id: data.roomId, username: socket.username, user_login: socket.userLogin,
      text: data.text||'', image: data.image||null, voice: data.voice||null, video: data.video||null,
      reply_to: data.replyTo||null, type: data.type||'text', timestamp: Date.now()
    };
    try {
      const res = await pool.query(
        'INSERT INTO room_messages (room_id, username, user_login, text, image, voice, video, reply_to, type, timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id',
        [msg.room_id, msg.username, msg.user_login, msg.text, msg.image, msg.voice, msg.video, msg.reply_to, msg.type, msg.timestamp]
      );
      msg.id = res.rows[0].id;
      io.to('room_' + data.roomId).emit('roomMessage', msg);
    } catch(e){ console.error(e); }
  });

  // === MESSAGES (GLOBAL & PM) ===
  socket.on('chatMessage', async (data) => { // GLOBAL
    if (!socket.username) return;
    const msg = { username: socket.username, text: data.text||'', image: data.image||null, voice: data.voice||null, video: data.video||null, type: data.type||'text', reply_to: data.replyTo||null, timestamp: Date.now() };
    try {
      const res = await pool.query('INSERT INTO messages (username,text,image,voice,video,reply_to,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id', 
        [msg.username, msg.text, msg.image, msg.voice, msg.video, msg.reply_to, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id; io.emit('chatMessage', msg);
    } catch (e) {}
  });

  socket.on('privateMessage', async (data) => { // PM
    if (!socket.userLogin) return;
    const msg = { from_login: socket.userLogin, to_login: data.toLogin, from_nickname: socket.username, text: data.text||'', image: data.image||null, voice: data.voice||null, video: data.video||null, reply_to: data.replyTo||null, type: data.type||'text', timestamp: Date.now() };
    try {
      const res = await pool.query('INSERT INTO private_messages (from_login,to_login,from_nickname,text,image,voice,video,reply_to,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id',
        [msg.from_login, msg.to_login, msg.from_nickname, msg.text, msg.image, msg.voice, msg.video, msg.reply_to, msg.type, msg.timestamp]);
      msg.id = res.rows[0].id;
      socket.emit('newPrivateMessage', msg);
      var targetUser = null;
      for (let [sid, info] of onlineUsers) { if(info.login === data.toLogin) { targetUser = socketUsers.get(sid); break; } }
      if (targetUser) { targetUser.emit('newPrivateMessage', msg); targetUser.emit('unreadNotification', {from: socket.userLogin}); }
    } catch(e){}
  });

  // BASIC STUFF
  socket.on('getUsers', async () => { if(socket.userLogin) {
    const res = await pool.query('SELECT login, nickname, avatar FROM users WHERE login != $1', [socket.userLogin]);
    socket.emit('usersList', res.rows);
  }});
  socket.on('searchUser', async (q) => { if(socket.userLogin && q) {
    const res = await pool.query('SELECT login, nickname, avatar FROM users WHERE LOWER(nickname) LIKE LOWER($1) AND login!=$2 LIMIT 10', ['%'+q+'%', socket.userLogin]);
    socket.emit('searchResults', res.rows);
  }});
  socket.on('getMyChats', async () => { if(!socket.userLogin) return;
    try {
      const res = await pool.query(`SELECT DISTINCT CASE WHEN from_login=$1 THEN to_login ELSE from_login END as other_login FROM private_messages WHERE from_login=$1 OR to_login=$1`, [socket.userLogin]);
      var logins = res.rows.map(r => r.other_login);
      if(logins.length===0) return socket.emit('myChats', []);
      var users = await pool.query('SELECT login, nickname, avatar FROM users WHERE login = ANY($1)', [logins]);
      var chats = [];
      for(var u of users.rows){
        var last = await pool.query('SELECT text,type,timestamp FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) ORDER BY timestamp DESC LIMIT 1', [socket.userLogin, u.login]);
        var unread = await pool.query('SELECT COUNT(*) as c FROM private_messages WHERE from_login=$1 AND to_login=$2 AND read=false', [u.login, socket.userLogin]);
        chats.push({login:u.login, nickname:u.nickname, avatar:u.avatar, lastMsg:last.rows[0], unread:parseInt(unread.rows[0].c)});
      }
      chats.sort((a,b)=>(b.lastMsg?b.lastMsg.timestamp:0)-(a.lastMsg?a.lastMsg.timestamp:0));
      socket.emit('myChats', chats);
    } catch(e){}
  });
  socket.on('getPrivateHistory', async(l)=>{ if(!socket.userLogin)return;
    const r=await pool.query('SELECT * FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) ORDER BY timestamp ASC LIMIT 200',[socket.userLogin,l]);
    await pool.query('UPDATE private_messages SET read=true WHERE from_login=$1 AND to_login=$2',[l,socket.userLogin]);
    socket.emit('privateHistory',{otherLogin:l,messages:r.rows});
  });
  socket.on('disconnect', () => { onlineUsers.delete(socket.id); socketUsers.delete(socket.id); sendOnlineToAll(); });
});

function sendOnlineToAll() {
  var list=[]; for(let [sid,i] of onlineUsers) list.push({nickname:i.nickname, login:i.login, avatar:i.avatar});
  for(let [sid] of onlineUsers){
    var s=socketUsers.get(sid);
    if(s) s.emit('onlineUsers', {count:list.length, users: (s.userRole==='admin'?list:[])});
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
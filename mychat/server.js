const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');
// ═══════════════════════════════════════════════
// AES-256-GCM MESSAGE ENCRYPTION
// Set MSG_SECRET env variable (32+ random chars) to enable
// ═══════════════════════════════════════════════
const MSG_SECRET = process.env.MSG_SECRET || null;
const ENCRYPT_ENABLED = !!MSG_SECRET;

function deriveKey() {
  // Derive a 32-byte key from the secret using SHA-256
  return crypto.createHash('sha256').update(MSG_SECRET).digest();
}

function encryptText(text) {
  if (!ENCRYPT_ENABLED || !text || typeof text !== 'string') return text;
  try {
    const key = deriveKey();
    const iv = crypto.randomBytes(12); // 96-bit IV for GCM
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const encrypted = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag(); // 16-byte authentication tag
    // Format: iv(12) + tag(16) + ciphertext — all base64
    return 'ENC:' + Buffer.concat([iv, tag, encrypted]).toString('base64');
  } catch(e) {
    console.error('[CRYPTO] encrypt error:', e.message);
    return text;
  }
}

function decryptText(text) {
  if (!ENCRYPT_ENABLED || !text || typeof text !== 'string') return text;
  if (!text.startsWith('ENC:')) return text; // not encrypted
  try {
    const key = deriveKey();
    const buf = Buffer.from(text.slice(4), 'base64');
    const iv = buf.slice(0, 12);
    const tag = buf.slice(12, 28);
    const ciphertext = buf.slice(28);
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    return decipher.update(ciphertext) + decipher.final('utf8');
  } catch(e) {
    console.error('[CRYPTO] decrypt error (wrong key or corrupted):', e.message);
    return '[зашифровано]';
  }
}

function encryptMsg(msg) {
  // Encrypt text fields of a message object before DB insert
  if (!ENCRYPT_ENABLED) return msg;
  const m = { ...msg };
  if (m.text) m.text = encryptText(m.text);
  if (m.reply_to_text) m.reply_to_text = encryptText(m.reply_to_text);
  return m;
}

function decryptMsg(msg) {
  // Decrypt text fields of a message object after DB read
  if (!msg) return msg;
  const m = { ...msg };
  if (m.text) m.text = decryptText(m.text);
  if (m.reply_to_text) m.reply_to_text = decryptText(m.reply_to_text);
  return m;
}

if (ENCRYPT_ENABLED) {
  console.log('[CRYPTO] Message encryption ENABLED (AES-256-GCM)');
} else {
  console.log('[CRYPTO] Message encryption DISABLED (set MSG_SECRET env to enable)');
}

let nodemailer = null;
try { nodemailer = require('nodemailer'); } catch(e) { console.log('nodemailer not installed — email disabled'); }
let OAuth2Client = null;
try { OAuth2Client = require('google-auth-library').OAuth2Client; } catch(e) { console.log('google-auth-library not installed — Google auth disabled'); }

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 25e6 }); // 25MB max (was 100MB)

app.use(express.static('public'));
app.use(express.json({ limit: '2mb' })); // limit JSON body size

// ── HTTP Security Headers ──────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=*, microphone=*, geolocation=*');
  next();
});

// ── HTTP Rate limiter (login/register endpoints if ever exposed via HTTP) ──
const httpHits = new Map(); // ip -> { count, resetAt }
app.use((req, res, next) => {
  if (req.path === '/config' || req.path.startsWith('/socket.io')) return next();
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  let h = httpHits.get(ip);
  if (!h || now > h.resetAt) { h = { count: 0, resetAt: now + 60_000 }; httpHits.set(ip, h); }
  h.count++;
  if (h.count > 300) { // 300 HTTP requests per minute per IP
    console.warn('[SECURITY] HTTP flood from', ip);
    return res.status(429).json({ error: 'Too Many Requests' });
  }
  next();
});

// Config endpoint — tells frontend what features are enabled
app.get('/config', (req, res) => {
  res.json({
    googleClientId: process.env.GOOGLE_CLIENT_ID || null,
    emailEnabled: EMAIL_ENABLED,
    telegramBotName: process.env.TELEGRAM_BOT_NAME || null,
    telegramBotId: process.env.TELEGRAM_BOT_ID || null
  });
});

// Telegram widget callback page
app.get('/tg-callback', (req, res) => {
  res.send(`<!DOCTYPE html><html><body><script>
    var data = \${JSON.stringify(req.query)};
    if (window.opener) {
      window.opener.postMessage({ type: 'telegram_auth', data: data }, '*');
      window.close();
    }
  <\/script></body></html>`);
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

const ADMIN_LOGIN = 'pekka';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || null;

// ── EMAIL CONFIG ──────────────────────────────────────────
// Set these env vars in Railway: EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASS, EMAIL_FROM
const EMAIL_ENABLED = !!(process.env.EMAIL_USER && process.env.EMAIL_PASS);
let mailer = null;
if (EMAIL_ENABLED && nodemailer) {
  mailer = nodemailer.createTransport({
    host: process.env.EMAIL_HOST || 'smtp.gmail.com',
    port: parseInt(process.env.EMAIL_PORT || '587'),
    secure: process.env.EMAIL_PORT === '465',
    auth: { user: process.env.EMAIL_USER, pass: process.env.EMAIL_PASS }
  });
}
async function sendEmail(to, subject, html) {
  if (!mailer) return false;
  try {
    await mailer.sendMail({ from: process.env.EMAIL_FROM || process.env.EMAIL_USER, to, subject, html });
    return true;
  } catch(e) { console.error('Email error:', e.message); return false; }
}

// ── GOOGLE OAUTH CONFIG ───────────────────────────────────
// Set GOOGLE_CLIENT_ID in Railway env vars
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || null;
const googleClient = (GOOGLE_CLIENT_ID && OAuth2Client) ? new OAuth2Client(GOOGLE_CLIENT_ID) : null;

// Email verification codes (in-memory, short TTL)
const pendingEmailVerifications = new Map(); // code -> { login, password, nickname, email, expiresAt }

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
    type VARCHAR(20) DEFAULT 'text', timestamp BIGINT NOT NULL
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS private_messages (
    id SERIAL PRIMARY KEY, from_login VARCHAR(50) NOT NULL,
    to_login VARCHAR(50) NOT NULL, from_nickname VARCHAR(100),
    text TEXT, image TEXT, voice TEXT,
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
  try { await pool.query("UPDATE users SET role='admin' WHERE login=$1", [ADMIN_LOGIN]); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(50) DEFAULT NULL UNIQUE'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to_id INT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to_text TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to_user TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS reply_to_id INT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS reply_to_text TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS reply_to_user TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS reply_to_id INT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS reply_to_text TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS reply_to_user TEXT DEFAULT NULL'); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS reactions (
    id SERIAL PRIMARY KEY,
    msg_type VARCHAR(20) NOT NULL,
    msg_id INT NOT NULL,
    user_login VARCHAR(50) NOT NULL,
    emoji VARCHAR(10) NOT NULL,
    UNIQUE(msg_type, msg_id, user_login)
  )`);
  // VIP system
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_until BIGINT DEFAULT 0'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_emoji TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen BIGINT DEFAULT 0'); } catch(e) {}
  try { await pool.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS user_login VARCHAR(50) DEFAULT NULL"); } catch(e) {}
  try { await pool.query("ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS user_login VARCHAR(50) DEFAULT NULL"); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS hide_online BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS read_at BIGINT DEFAULT NULL'); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS pinned_chats (
    id SERIAL PRIMARY KEY,
    user_login VARCHAR(50) NOT NULL,
    chat_type VARCHAR(20) NOT NULL,
    chat_id VARCHAR(100) NOT NULL,
    pinned_at BIGINT DEFAULT 0,
    UNIQUE(user_login, chat_type, chat_id)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS vip_codes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(32) UNIQUE NOT NULL,
    duration_days INT NOT NULL DEFAULT 30,
    used BOOLEAN DEFAULT false,
    used_by VARCHAR(50) DEFAULT NULL,
    created_at BIGINT DEFAULT 0
  )`);

  // === NEW FEATURES ===
  // Verification badge
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS verified BOOLEAN DEFAULT false'); } catch(e) {}
  // Promo codes for verification
  await pool.query(`CREATE TABLE IF NOT EXISTS verify_codes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(32) UNIQUE NOT NULL,
    used BOOLEAN DEFAULT false,
    used_by VARCHAR(50) DEFAULT NULL,
    created_at BIGINT DEFAULT 0
  )`);
  // Muted chats
  await pool.query(`CREATE TABLE IF NOT EXISTS muted_chats (
    id SERIAL PRIMARY KEY,
    user_login VARCHAR(50) NOT NULL,
    chat_type VARCHAR(20) NOT NULL,
    chat_id VARCHAR(100) NOT NULL,
    muted_until BIGINT NOT NULL,
    UNIQUE(user_login, chat_type, chat_id)
  )`);
  // Message edits
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS edited BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS edited BOOLEAN DEFAULT false'); } catch(e) {}
  // Forwarded messages
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS fwd_from_nick TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS fwd_from_nick TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS fwd_from_nick TEXT DEFAULT NULL'); } catch(e) {}
  // Saved messages (special PM to self — no extra table needed)
  // Group read receipts
  await pool.query(`CREATE TABLE IF NOT EXISTS room_message_reads (
    id SERIAL PRIMARY KEY,
    msg_id INT NOT NULL,
    user_login VARCHAR(50) NOT NULL,
    read_at BIGINT NOT NULL,
    UNIQUE(msg_id, user_login)
  )`);
  // Per-dialog chat background
  await pool.query(`CREATE TABLE IF NOT EXISTS dialog_bg (
    id SERIAL PRIMARY KEY,
    user_login VARCHAR(50) NOT NULL,
    chat_type VARCHAR(20) NOT NULL,
    chat_id VARCHAR(100) NOT NULL,
    bg_id VARCHAR(50) NOT NULL DEFAULT 'none',
    bg_data TEXT DEFAULT NULL,
    UNIQUE(user_login, chat_type, chat_id)
  )`);

  // Email verification
  await pool.query(`CREATE TABLE IF NOT EXISTS pending_registrations (
    id SERIAL PRIMARY KEY,
    login VARCHAR(50) NOT NULL,
    nickname VARCHAR(50) NOT NULL,
    email VARCHAR(200) NOT NULL,
    password_hash VARCHAR(200) NOT NULL,
    code VARCHAR(10) NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL
  )`);
  // User email field
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(200) DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS google_id VARCHAR(200) DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS telegram_id VARCHAR(100) DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_method VARCHAR(20) DEFAULT \'password\''); } catch(e) {}
  // Stories
  await pool.query(`CREATE TABLE IF NOT EXISTS stories (
    id SERIAL PRIMARY KEY,
    user_login VARCHAR(50) NOT NULL,
    user_nickname VARCHAR(100) NOT NULL,
    media_url TEXT NOT NULL,
    media_type VARCHAR(20) DEFAULT 'image',
    text TEXT DEFAULT NULL,
    timestamp BIGINT NOT NULL,
    expires_at BIGINT NOT NULL
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS story_views (
    id SERIAL PRIMARY KEY,
    story_id INT NOT NULL,
    viewer_login VARCHAR(50) NOT NULL,
    viewed_at BIGINT NOT NULL,
    UNIQUE(story_id, viewer_login)
  )`);
  // File/video attachments
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS file_url TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS file_name TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE private_messages ADD COLUMN IF NOT EXISTS file_size BIGINT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_url TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_size BIGINT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS file_url TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS file_name TEXT DEFAULT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE room_messages ADD COLUMN IF NOT EXISTS file_size BIGINT DEFAULT NULL'); } catch(e) {}

  // Blocked users
  await pool.query(`CREATE TABLE IF NOT EXISTS blocked_users (
    id SERIAL PRIMARY KEY,
    blocker_login VARCHAR(50) NOT NULL,
    blocked_login VARCHAR(50) NOT NULL,
    created_at BIGINT NOT NULL DEFAULT 0,
    UNIQUE(blocker_login, blocked_login)
  )`);

  console.log('Database ready');
}
initDB();

const onlineUsers = new Map();
const socketUsers = new Map();
const activeCalls = new Map();   // callerLogin -> { calleeLogin, callType, answered }
const callTimeouts = new Map();  // callerLogin -> { timeout, calleeLogin }

// ── IMAGE SERIALIZATION HELPERS ──────────────────────────
// Images can be a single base64 string or array of base64 strings
// We always store as JSON string in DB to handle both cases consistently
function serializeImage(img) {
  if (!img) return null;
  if (Array.isArray(img)) {
    return JSON.stringify(img); // store array as JSON string
  }
  return img; // single image stored as-is (still valid JSON string if needed)
}

function deserializeImage(imgStr) {
  if (!imgStr) return null;
  if (imgStr.startsWith('[')) {
    try { return JSON.parse(imgStr); } catch(e) { return imgStr; }
  }
  return imgStr; // single image
}

// Apply deserialize to a message row (mutates in place)
function fixMsgImages(msg) {
  if (!msg) return msg;
  if (msg.image) msg.image = deserializeImage(msg.image);
  // Decrypt text fields from DB
  return decryptMsg(msg);
}

function getIP(socket) {
  return socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || 'unknown';
}

async function addLog(action, username, detail, ip) {
  try { await pool.query('INSERT INTO logs (action,username,detail,ip,timestamp) VALUES ($1,$2,$3,$4,$5)',
    [action, username||'', detail||'', ip||'', Date.now()]); } catch(e) {}
}

function anonymizeVotes(votes) {
  const counts = {};
  Object.values(votes||{}).forEach(function(v) {
    var ids = Array.isArray(v) ? v : [v];
    ids.forEach(function(id) { counts[id] = (counts[id] || 0) + 1; });
  });
  return { __anonymous: true, counts };
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


// ═══════════════════════════════════════════════
// SECURITY LAYER
// ═══════════════════════════════════════════════

// --- Rate limiter (in-memory, per IP) ---
const rateLimitMap = new Map(); // ip -> { events: [timestamps], blocked_until }
const RATE_RULES = {
  login:    { max: 5,  windowMs: 60_000,  blockMs: 300_000 },  // 5 попыток / мин → бан 5 мин
  register: { max: 3,  windowMs: 60_000,  blockMs: 600_000 },  // 3 регистрации / мин → бан 10 мин
  message:  { max: 30, windowMs: 10_000,  blockMs: 30_000  },  // 30 сообщений / 10с → бан 30с
  socket:   { max: 100,windowMs: 10_000,  blockMs: 60_000  },  // 100 событий / 10с → бан 1 мин
};

function checkRateLimit(ip, action) {
  const rule = RATE_RULES[action];
  if (!rule) return true;
  const key = ip + ':' + action;
  const now = Date.now();
  let entry = rateLimitMap.get(key);
  if (!entry) { entry = { events: [], blocked_until: 0 }; rateLimitMap.set(key, entry); }
  if (entry.blocked_until > now) return false; // заблокирован
  entry.events = entry.events.filter(t => now - t < rule.windowMs);
  entry.events.push(now);
  if (entry.events.length > rule.max) {
    entry.blocked_until = now + rule.blockMs;
    entry.events = [];
    console.warn('[SECURITY] Rate limit hit:', action, 'IP:', ip);
    return false;
  }
  return true;
}

// Cleanup rate limit map every 10 minutes
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of rateLimitMap) {
    if (entry.blocked_until < now && entry.events.length === 0) rateLimitMap.delete(key);
  }
}, 600_000);

// --- Input sanitizer ---
function sanitize(str, maxLen) {
  if (typeof str !== 'string') return '';
  return str.slice(0, maxLen || 1000);
}

function stripHtml(str) {
  if (typeof str !== 'string') return '';
  return str.replace(/<[^>]*>/g, '');
}

// --- Socket event rate limiter wrapper ---
function guardedOn(socket, event, handler) {
  socket.on(event, function(...args) {
    const ip = getIP(socket);
    if (!checkRateLimit(ip, 'socket')) {
      return socket.emit('rateLimited', { msg: 'Слишком много запросов, подождите.' });
    }
    return handler(...args);
  });
}

io.on('connection', (socket) => {
  const ip = getIP(socket);

  // ── Per-socket global event flood guard ──────────────────
  let _socketEventCount = 0;
  let _socketEventReset = Date.now() + 10_000;
  let _socketBlocked = false;

  socket.onAny((event) => {
    const now = Date.now();
    if (now > _socketEventReset) {
      _socketEventCount = 0;
      _socketEventReset = now + 10_000;
      _socketBlocked = false;
    }
    _socketEventCount++;
    if (_socketEventCount > 150) { // 150 events per 10s per socket
      if (!_socketBlocked) {
        console.warn('[SECURITY] Socket flood from', ip, 'event:', event);
        socket.emit('rateLimited', { msg: 'Слишком много запросов. Подождите.' });
        _socketBlocked = true;
        // Disconnect repeat offenders
        if (_socketEventCount > 500) {
          console.warn('[SECURITY] Disconnecting flood socket', ip);
          socket.disconnect(true);
        }
      }
    }
  });
  // ─────────────────────────────────────────────────────────

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
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login: user.login, token: token, avatar: user.avatar || null, username: user.username || null, verified: user.verified || false, vip_until: user.vip_until || 0, vip_emoji: user.vip_emoji || null });
      sendOnlineToAll();
    } catch(e) { console.error(e); socket.emit('authError', 'Ошибка авто-входа'); }
  });

  // ── REGISTER WITH EMAIL VERIFICATION ─────────────────────
  socket.on('register', async ({ login, password, nickname, email }) => {
    if (!checkRateLimit(ip, 'register')) return socket.emit('authError', 'Слишком много регистраций с вашего IP. Подождите.');
    try {
      if (!login || !password || !nickname) return socket.emit('authError', 'Заполни все поля');
      login = sanitize(login, 50); password = sanitize(password, 200); nickname = sanitize(nickname, 50); email = sanitize(email, 200);
      login = login.trim().toLowerCase();
      nickname = nickname.trim();
      email = (email || '').trim().toLowerCase();

      // Validate login
      if (!/^[a-z0-9_]{3,30}$/.test(login)) return socket.emit('authError', 'Логин: 3-30 символов, только a-z, 0-9, _');
      if (password.length < 6) return socket.emit('authError', 'Пароль минимум 6 символов');
      if (nickname.length < 2 || nickname.length > 30) return socket.emit('authError', 'Ник 2-30 символов');

      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) return socket.emit('authError', 'Этот логин уже занят');
      const nickExists = await pool.query('SELECT id FROM users WHERE LOWER(nickname)=LOWER($1)', [nickname]);
      if (nickExists.rows.length > 0) return socket.emit('authError', 'Этот ник уже занят');

      const hash = await bcrypt.hash(password, 10);

      // If email provided and mailer configured → send verification code
      if (email && EMAIL_ENABLED) {
        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return socket.emit('authError', 'Неверный формат email');
        const emailExists = await pool.query('SELECT id FROM users WHERE email=$1', [email]);
        if (emailExists.rows.length > 0) return socket.emit('authError', 'Email уже используется');

        // Clean old pending
        await pool.query('DELETE FROM pending_registrations WHERE login=$1 OR email=$2', [login, email]);

        const code = String(Math.floor(100000 + Math.random() * 900000)); // 6-digit
        const now = Date.now();
        const expires = now + 15 * 60000; // 15 min
        await pool.query('INSERT INTO pending_registrations (login,nickname,email,password_hash,code,created_at,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7)',
          [login, nickname, email, hash, code, now, expires]);

        const html = `<div style="font-family:Arial,sans-serif;max-width:400px;margin:0 auto;">
          <h2 style="color:#2ea9df;">MyChat — Подтверждение регистрации</h2>
          <p>Привет, <b>\${nickname}</b>! Для завершения регистрации введи этот код:</p>
          <div style="font-size:36px;font-weight:bold;letter-spacing:8px;text-align:center;padding:20px;background:#f0f4f8;border-radius:12px;margin:20px 0;">\${code}</div>
          <p style="color:#666;">Код действителен 15 минут. Если ты не регистрировался — проигнорируй это письмо.</p>
        </div>`;

        const sent = await sendEmail(email, 'MyChat — Код подтверждения: ' + code, html);
        if (!sent) return socket.emit('authError', 'Не удалось отправить email. Попробуй без почты.');

        socket.emit('emailVerificationRequired', { login, email });
        return;
      }

      // Registration without email (or email disabled)
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      const token = generateToken();
      await pool.query('INSERT INTO users (login,password,nickname,banned,muted_until,role,token,email,email_verified,auth_method) VALUES ($1,$2,$3,false,0,$4,$5,$6,$7,$8)',
        [login, hash, nickname, role, token, email || null, email ? false : null, 'password']);
      socket.username = nickname; socket.userLogin = login; socket.userRole = role;
      onlineUsers.set(socket.id, { nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role, login, token, vip_until: 0, vip_emoji: null, verified: false });
      sendOnlineToAll();
      await addLog('register', nickname, 'Registered', ip);
    } catch (e) { console.error(e); socket.emit('authError', 'Ошибка регистрации'); }
  });

  // ── CONFIRM EMAIL CODE ──────────────────────────────────
  socket.on('confirmEmailCode', async ({ login, code }) => {
    try {
      const row = await pool.query('SELECT * FROM pending_registrations WHERE login=$1 AND code=$2', [login, code.trim()]);
      if (!row.rows.length) return socket.emit('emailCodeError', 'Неверный код');
      const pending = row.rows[0];
      if (Date.now() > pending.expires_at) {
        await pool.query('DELETE FROM pending_registrations WHERE id=$1', [pending.id]);
        return socket.emit('emailCodeError', 'Код истёк. Зарегистрируйся снова');
      }
      // Create user
      const role = pending.login === ADMIN_LOGIN ? 'admin' : 'user';
      const token = generateToken();
      await pool.query('INSERT INTO users (login,password,nickname,banned,muted_until,role,token,email,email_verified,auth_method) VALUES ($1,$2,$3,false,0,$4,$5,$6,true,$7)',
        [pending.login, pending.password_hash, pending.nickname, role, token, pending.email, 'email']);
      await pool.query('DELETE FROM pending_registrations WHERE id=$1', [pending.id]);
      socket.username = pending.nickname; socket.userLogin = pending.login; socket.userRole = role;
      onlineUsers.set(socket.id, { nickname: pending.nickname, login: pending.login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname: pending.nickname, role, login: pending.login, token, vip_until: 0, vip_emoji: null, verified: false });
      sendOnlineToAll();
      await addLog('register', pending.nickname, 'Registered via email', ip);
    } catch(e) { console.error(e); socket.emit('emailCodeError', 'Ошибка сервера'); }
  });

  // ── RESEND EMAIL CODE ──────────────────────────────────
  socket.on('resendEmailCode', async ({ login }) => {
    try {
      const row = await pool.query('SELECT * FROM pending_registrations WHERE login=$1', [login]);
      if (!row.rows.length) return socket.emit('emailCodeError', 'Заявка не найдена');
      const pending = row.rows[0];
      const code = String(Math.floor(100000 + Math.random() * 900000));
      const expires = Date.now() + 15 * 60000;
      await pool.query('UPDATE pending_registrations SET code=$1, expires_at=$2 WHERE id=$3', [code, expires, pending.id]);
      const html = `<h2 style="color:#2ea9df;">MyChat — Новый код</h2><p>Твой новый код: <b style="font-size:24px;">\${code}</b></p><p>Действителен 15 минут.</p>`;
      const sent = await sendEmail(pending.email, 'MyChat — Новый код: ' + code, html);
      if (sent) socket.emit('emailCodeResent', { ok: true });
      else socket.emit('emailCodeError', 'Не удалось отправить письмо');
    } catch(e) { socket.emit('emailCodeError', 'Ошибка'); }
  });

  // ── TELEGRAM OAUTH ──────────────────────────────────────
  socket.on('telegramAuth', async (tgData) => {
    if (!TELEGRAM_BOT_TOKEN) return socket.emit('authError', 'Telegram авторизация не настроена');
    try {
      // Verify Telegram hash
      const { hash, ...dataWithout } = tgData;
      const checkArr = Object.keys(dataWithout).sort().map(k => k + '=' + dataWithout[k]);
      const checkStr = checkArr.join('\n');
      const secretKey = crypto.createHash('sha256').update(TELEGRAM_BOT_TOKEN).digest();
      const hmac = crypto.createHmac('sha256', secretKey).update(checkStr).digest('hex');
      if (hmac !== hash) return socket.emit('authError', 'Неверная подпись Telegram');
      if (Date.now() / 1000 - tgData.auth_date > 86400) return socket.emit('authError', 'Данные Telegram устарели');

      const tgId = String(tgData.id);
      const tgName = [tgData.first_name, tgData.last_name].filter(Boolean).join(' ') || tgData.username || 'User';

      // Find existing user by telegram_id
      let user = await pool.query('SELECT * FROM users WHERE telegram_id=$1', [tgId]);
      if (user.rows.length > 0) {
        const u = user.rows[0];
        if (u.banned) return socket.emit('authError', 'Аккаунт заблокирован');
        const token = generateToken();
        await pool.query('UPDATE users SET token=$1 WHERE id=$2', [token, u.id]);
        socket.username = u.nickname; socket.userLogin = u.login;
        socket.userRole = u.login === ADMIN_LOGIN ? 'admin' : (u.role || 'user');
        onlineUsers.set(socket.id, { nickname: u.nickname, login: u.login, ip });
        socketUsers.set(socket.id, socket);
        socket.emit('authSuccess', { nickname: u.nickname, role: socket.userRole, login: u.login, token, avatar: u.avatar || null, username: u.username || null, verified: u.verified || false, vip_until: u.vip_until || 0, vip_emoji: u.vip_emoji || null });
        sendOnlineToAll();
        await addLog('login', u.nickname, 'Login via Telegram', ip);
      } else {
        // New user — need to pick login/nickname
        socket.emit('telegramNeedSetup', { tgId, suggestedName: tgName, tgUsername: tgData.username || null });
      }
    } catch(e) { console.error('Telegram auth error:', e); socket.emit('authError', 'Ошибка Telegram авторизации'); }
  });

  socket.on('telegramRegisterSetup', async ({ tgId, login, nickname }) => {
    try {
      login = (login || '').trim().toLowerCase();
      nickname = (nickname || '').trim();
      if (!/^[a-z0-9_]{3,30}$/.test(login)) return socket.emit('authError', 'Логин: 3-30 символов, только a-z, 0-9, _');
      if (nickname.length < 2 || nickname.length > 30) return socket.emit('authError', 'Ник 2-30 символов');
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) return socket.emit('authError', 'Логин уже занят');
      const nickExists = await pool.query('SELECT id FROM users WHERE LOWER(nickname)=LOWER($1)', [nickname]);
      if (nickExists.rows.length > 0) return socket.emit('authError', 'Ник уже занят');
      const fakeHash = await bcrypt.hash(tgId + Date.now(), 8);
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      const token = generateToken();
      await pool.query('INSERT INTO users (login,password,nickname,banned,muted_until,role,token,telegram_id,auth_method) VALUES ($1,$2,$3,false,0,$4,$5,$6,$7,$8)',
        [login, fakeHash, nickname, role, token, tgId, 'telegram']);
      socket.username = nickname; socket.userLogin = login; socket.userRole = role;
      onlineUsers.set(socket.id, { nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role, login, token, vip_until: 0, vip_emoji: null, verified: false });
      sendOnlineToAll();
      await addLog('register', nickname, 'Registered via Telegram', ip);
    } catch(e) { console.error(e); socket.emit('authError', 'Ошибка регистрации'); }
  });

  // ── GOOGLE OAUTH LOGIN/REGISTER ─────────────────────────
  socket.on('googleAuth', async ({ idToken }) => {
    if (!googleClient) return socket.emit('authError', 'Google авторизация не настроена');
    try {
      const ticket = await googleClient.verifyIdToken({ idToken, audience: GOOGLE_CLIENT_ID });
      const payload = ticket.getPayload();
      const googleId = payload.sub;
      const googleEmail = (payload.email || '').toLowerCase();
      const googleName = payload.name || payload.given_name || 'User';

      // Check if user exists by google_id
      let user = await pool.query('SELECT * FROM users WHERE google_id=$1', [googleId]);
      if (!user.rows.length) {
        // Check by email
        user = await pool.query('SELECT * FROM users WHERE email=$1', [googleEmail]);
      }

      if (user.rows.length > 0) {
        // Existing user — login
        const u = user.rows[0];
        if (u.banned) return socket.emit('authError', 'Аккаунт заблокирован');
        if (!u.google_id) await pool.query('UPDATE users SET google_id=$1 WHERE id=$2', [googleId, u.id]);
        const token = generateToken();
        await pool.query('UPDATE users SET token=$1 WHERE id=$2', [token, u.id]);
        socket.username = u.nickname; socket.userLogin = u.login; socket.userRole = u.login === ADMIN_LOGIN ? 'admin' : (u.role || 'user');
        onlineUsers.set(socket.id, { nickname: u.nickname, login: u.login, ip });
        socketUsers.set(socket.id, socket);
        socket.emit('authSuccess', { nickname: u.nickname, role: socket.userRole, login: u.login, token, avatar: u.avatar || null, username: u.username || null, verified: u.verified || false, vip_until: u.vip_until || 0, vip_emoji: u.vip_emoji || null });
        sendOnlineToAll();
        await addLog('login', u.nickname, 'Login via Google', ip);
      } else {
        // New user — need to pick a login/nickname
        socket.emit('googleNeedSetup', { googleId, email: googleEmail, suggestedName: googleName });
      }
    } catch(e) { console.error('Google auth error:', e); socket.emit('authError', 'Ошибка Google авторизации: ' + e.message); }
  });

  socket.on('googleRegisterSetup', async ({ googleId, email, login, nickname }) => {
    try {
      login = (login || '').trim().toLowerCase();
      nickname = (nickname || '').trim();
      if (!/^[a-z0-9_]{3,30}$/.test(login)) return socket.emit('authError', 'Логин: 3-30 символов, только a-z, 0-9, _');
      if (nickname.length < 2 || nickname.length > 30) return socket.emit('authError', 'Ник 2-30 символов');
      const exists = await pool.query('SELECT id FROM users WHERE login=$1', [login]);
      if (exists.rows.length > 0) return socket.emit('authError', 'Логин уже занят');
      const nickExists = await pool.query('SELECT id FROM users WHERE LOWER(nickname)=LOWER($1)', [nickname]);
      if (nickExists.rows.length > 0) return socket.emit('authError', 'Ник уже занят');

      const fakeHash = await bcrypt.hash(googleId + Date.now(), 8); // unused password
      const role = login === ADMIN_LOGIN ? 'admin' : 'user';
      const token = generateToken();
      await pool.query('INSERT INTO users (login,password,nickname,banned,muted_until,role,token,email,email_verified,google_id,auth_method) VALUES ($1,$2,$3,false,0,$4,$5,$6,true,$7,$8)',
        [login, fakeHash, nickname, role, token, email || null, googleId, 'google']);
      socket.username = nickname; socket.userLogin = login; socket.userRole = role;
      onlineUsers.set(socket.id, { nickname, login, ip });
      socketUsers.set(socket.id, socket);
      socket.emit('authSuccess', { nickname, role, login, token, vip_until: 0, vip_emoji: null, verified: false });
      sendOnlineToAll();
      await addLog('register', nickname, 'Registered via Google', ip);
    } catch(e) { console.error(e); socket.emit('authError', 'Ошибка регистрации'); }
  });

  socket.on('login', async ({ login, password }) => {
    if (!checkRateLimit(ip, 'login')) return socket.emit('authError', 'Слишком много попыток. Подождите 5 минут.');
    try {
      if (!login || !password) return socket.emit('authError', 'Заполни все поля');
      login = sanitize(login, 50); password = sanitize(password, 200);
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
      socket.emit('authSuccess', { nickname: user.nickname, role: socket.userRole, login, token, avatar: user.avatar || null, username: user.username || null, verified: user.verified || false, vip_until: user.vip_until || 0, vip_emoji: user.vip_emoji || null });
      sendOnlineToAll();
      await addLog('login', user.nickname, 'Login', ip);
    } catch (e) { console.error(e); socket.emit('authError', 'Ошибка входа'); }
  });

  // === WEBRTC CALLS ===

  async function saveCallLog({ callerLogin, callerNick, calleeLogin, callType, answered, duration, missed }) {
    try {
      const calleeRes = await pool.query('SELECT nickname FROM users WHERE login=$1', [calleeLogin]);
      const calleeNick = calleeRes.rows[0]?.nickname || calleeLogin;
      const ts = Date.now();
      const textVal = JSON.stringify({ callType, answered, duration: duration || 0, missed: !!missed, callerLogin, callerNick, calleeLogin, calleeNick });
      const r1 = await pool.query(
        'INSERT INTO private_messages (from_login,to_login,from_nickname,text,type,timestamp,read) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id',
        [callerLogin, calleeLogin, callerNick, textVal, 'call_log', ts, true]
      );
      const r2 = await pool.query(
        'INSERT INTO private_messages (from_login,to_login,from_nickname,text,type,timestamp,read) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id',
        [calleeLogin, callerLogin, calleeNick, textVal, 'call_log', ts, false]
      );
      const msg1 = { id: r1.rows[0].id, from_login: callerLogin, to_login: calleeLogin, from_nickname: callerNick, text: textVal, type: 'call_log', timestamp: ts, read: true };
      const msg2 = { id: r2.rows[0].id, from_login: calleeLogin, to_login: callerLogin, from_nickname: calleeNick, text: textVal, type: 'call_log', timestamp: ts, read: false };
      const callerSocket = findSocketByLogin(callerLogin);
      const calleeSocket = findSocketByLogin(calleeLogin);
      if (callerSocket) callerSocket.emit('newPrivateMessage', msg1);
      if (calleeSocket) calleeSocket.emit('newPrivateMessage', msg2);
    } catch(e) { console.error('saveCallLog error', e); }
  }

  socket.on('callUser', ({ userToCall, signalData, callType }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(userToCall);
    if (!targetSocket) {
      socket.emit('callEnded', { reason: 'offline' });
      return;
    }

    activeCalls.set(socket.userLogin, { calleeLogin: userToCall, callType: callType || 'video', answered: false });

    targetSocket.emit('incomingCall', {
      signal: signalData,
      from: socket.userLogin,
      fromNickname: socket.username,
      callType: callType || 'video'
    });

    // Auto-reject after 20 seconds if not answered
    const timeout = setTimeout(async () => {
      const info = activeCalls.get(socket.userLogin);
      if (!info || info.answered) return; // already answered or cleaned up
      activeCalls.delete(socket.userLogin);
      callTimeouts.delete(socket.userLogin);

      // Notify both sides
      socket.emit('callEnded', { reason: 'timeout' });
      targetSocket.emit('callEnded', { reason: 'timeout' });

      // Save as missed call log
      await saveCallLog({
        callerLogin: socket.userLogin,
        callerNick: socket.username,
        calleeLogin: userToCall,
        callType: callType || 'video',
        answered: false,
        duration: 0,
        missed: true,
      });
    }, 20000);

    callTimeouts.set(socket.userLogin, { timeout, calleeLogin: userToCall });
  });

  socket.on('answerCall', ({ signal, to }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(to);
    if (targetSocket) {
      targetSocket.emit('callAccepted', signal);
      // Mark as answered so auto-reject doesn't fire
      if (activeCalls.has(to)) activeCalls.get(to).answered = true;
      // Clear the 20s timeout
      const t = callTimeouts.get(to);
      if (t) { clearTimeout(t.timeout); callTimeouts.delete(to); }
    }
  });

  socket.on('hangUp', async ({ to, duration, rejected }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(to);
    if (targetSocket) targetSocket.emit('callEnded', { reason: 'hangup' });

    // Clear auto-reject timeout if caller hangs up manually
    const t = callTimeouts.get(socket.userLogin);
    if (t) { clearTimeout(t.timeout); callTimeouts.delete(socket.userLogin); }

    const callInfo = activeCalls.get(socket.userLogin) || activeCalls.get(to);
    const callType = callInfo ? callInfo.callType : 'audio';
    const answered = callInfo ? callInfo.answered : false;
    const dur = answered ? (duration || 0) : 0;

    // missed = callee rejected (duration=0, not answered) OR caller hung up before answer
    const missed = !answered;

    activeCalls.delete(socket.userLogin);
    activeCalls.delete(to);

    // Determine who is caller: if we have activeCalls entry for socket.userLogin, we are caller
    // If callee rejects, callInfo will be under the caller's login (to)
    let callerLogin, calleeLogin, callerNick;
    if (callInfo && callInfo.calleeLogin === to) {
      // socket is the caller
      callerLogin = socket.userLogin;
      calleeLogin = to;
      callerNick = socket.username;
    } else {
      // socket is the callee rejecting
      callerLogin = to;
      calleeLogin = socket.userLogin;
      callerNick = null; // will be fetched in saveCallLog
      // fetch caller nick
      try {
        const cr = await pool.query('SELECT nickname FROM users WHERE login=$1', [to]);
        callerNick = cr.rows[0]?.nickname || to;
      } catch(e) { callerNick = to; }
    }

    await saveCallLog({ callerLogin, callerNick, calleeLogin, callType, answered, duration: dur, missed });
  });

  socket.on('iceCandidate', ({ candidate, to }) => {
    if (!socket.userLogin) return;
    const targetSocket = findSocketByLogin(to);
    if (targetSocket) targetSocket.emit('iceCandidate', candidate);
  });

  // === GENERAL CHAT ===
  socket.on('getGeneralHistory', async () => {
    if (!socket.userLogin) return;
    try {
      const msgs = await pool.query('SELECT * FROM messages ORDER BY timestamp ASC LIMIT 200');
      socket.emit('messageHistory', msgs.rows.map(fixMsgImages));
    } catch(e) { console.error(e); }
  });

  socket.on('chatMessage', async (data) => {
    if (!checkRateLimit(ip, 'message')) return socket.emit('rateLimited', { msg: 'Слишком быстро! Помедленнее.' });
    if (!socket.userLogin) return;
    if (data && data.text) data.text = sanitize(data.text, 4000);
    if (!socket.username) return;
    try {
      const u = await pool.query('SELECT muted_until FROM users WHERE login=$1', [socket.userLogin]);
      if (u.rows.length > 0 && u.rows[0].muted_until > Date.now()) return socket.emit('chatError', 'Вы замучены');
    } catch(e) {}
    // Get vip_emoji for sender
    let vipEmoji = null;
    try { const ve = await pool.query('SELECT vip_emoji, vip_until FROM users WHERE login=$1', [socket.userLogin]); if (ve.rows[0] && ve.rows[0].vip_until > Date.now()) vipEmoji = ve.rows[0].vip_emoji || null; } catch(e) {}
    const msg = { username: socket.username, user_login: socket.userLogin, vip_emoji: vipEmoji, text: data.text||'', image: serializeImage(data.image)||null, voice: data.voice||null, type: data.type||'text', timestamp: Date.now(), reply_to_id: data.reply_to_id||null, reply_to_text: data.reply_to_text||null, reply_to_user: data.reply_to_user||null, file_url: data.file_url||null, file_name: data.file_name||null, file_size: data.file_size||null };
    try {
      if (data.text) data.text = encryptText(data.text);
      if (data.reply_to_text) data.reply_to_text = encryptText(data.reply_to_text);
      const res = await pool.query('INSERT INTO messages (username,user_login,text,image,voice,type,timestamp,reply_to_id,reply_to_text,reply_to_user,file_url,file_name,file_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id',
        [msg.username, msg.user_login, msg.text, msg.image, msg.voice, msg.type, msg.timestamp, msg.reply_to_id, msg.reply_to_text, msg.reply_to_user, msg.file_url, msg.file_name, msg.file_size]);
      msg.id = res.rows[0].id;
      io.emit('chatMessage', fixMsgImages({...msg}));
      // Detect @mentions and notify mentioned users
      if (msg.text) {
        const mentionMatches = msg.text.match(/@([a-zA-Zа-яА-ЯёЁ0-9_]+)/g);
        if (mentionMatches) {
          const mentionNames = mentionMatches.map(m => m.slice(1).toLowerCase());
          const allUsers = await pool.query('SELECT login, nickname, username FROM users WHERE login != $1', [socket.userLogin]);
          allUsers.rows.forEach(u => {
            if (mentionNames.includes(u.nickname.toLowerCase()) || mentionNames.includes(u.login.toLowerCase()) || (u.username && mentionNames.includes(u.username.toLowerCase()))) {
              const ts = findSocketByLogin(u.login);
              if (ts) ts.emit('mentionReceived', { from: socket.username, text: msg.text, chatType: 'general', chatId: 'general', msgId: msg.id, ts: msg.timestamp });
            }
          });
        }
      }
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

  socket.on('setAvatar', async (avatarData) => {
    if (!socket.userLogin) return;
    try {
      // limit avatar size (~2MB base64)
      if (avatarData && avatarData.length > 2 * 1024 * 1024 * 1.37) return socket.emit('chatError', 'Аватарка слишком большая (макс. 2МБ)');
      await pool.query('UPDATE users SET avatar=$1 WHERE login=$2', [avatarData || null, socket.userLogin]);
      socket.emit('avatarChanged', avatarData || null);
      // notify contacts about avatar change
      io.emit('userAvatarUpdated', { login: socket.userLogin, avatar: avatarData || null });
    } catch(e) { console.error(e); socket.emit('chatError', 'Ошибка при смене аватарки'); }
  });

  socket.on('setUsername', async (username) => {
    if (!socket.userLogin) return;
    if (!username) {
      // allow clearing username
      await pool.query('UPDATE users SET username=NULL WHERE login=$1', [socket.userLogin]).catch(()=>{});
      return socket.emit('usernameChanged', null);
    }
    username = username.trim().replace(/[^a-zA-Z0-9_]/g, '');
    if (username.length < 3 || username.length > 32) return socket.emit('chatError', 'Юзернейм: от 3 до 32 символов (латиница, цифры, _)');
    try {
      const exists = await pool.query('SELECT login FROM users WHERE LOWER(username)=LOWER($1) AND login!=$2', [username, socket.userLogin]);
      if (exists.rows.length > 0) return socket.emit('chatError', 'Этот юзернейм уже занят');
      await pool.query('UPDATE users SET username=$1 WHERE login=$2', [username, socket.userLogin]);
      socket.emit('usernameChanged', username);
    } catch(e) { console.error(e); socket.emit('chatError', 'Ошибка при смене юзернейма'); }
  });

  socket.on('searchUser', async (query) => {
    if (!socket.userLogin || !query) return;
    try {
      // search by nickname OR username
      const res = await pool.query(
        'SELECT login, nickname, username, avatar FROM users WHERE (LOWER(nickname) LIKE LOWER($1) OR LOWER(username) LIKE LOWER($2)) AND login != $3 LIMIT 15',
        ['%' + query + '%', '%' + query.replace('@','') + '%', socket.userLogin]
      );
      socket.emit('searchResults', res.rows);
    } catch(e) { socket.emit('searchResults', []); }
  });

  socket.on('getUserProfile', async (login) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT login, nickname, username, avatar, verified, vip_emoji, vip_until FROM users WHERE login=$1', [login]);
      if (res.rows.length > 0) {
        const u = res.rows[0];
        socket.emit('userProfile', {
          login: u.login, nickname: u.nickname, username: u.username, avatar: u.avatar,
          verified: u.verified || false,
          vip_emoji: (u.vip_until > Date.now() ? u.vip_emoji : null) || null,
          vip_until: u.vip_until || 0
        });
      }
    } catch(e) {}
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
      // Get all chats with last message and unread count in efficient queries
      const res = await pool.query(
        'SELECT DISTINCT CASE WHEN from_login=$1 THEN to_login ELSE from_login END as other_login FROM private_messages WHERE (from_login=$1 OR to_login=$1) AND from_login != to_login',
        [socket.userLogin]
      );
      var logins = res.rows.map(r => r.other_login).filter(l => l !== socket.userLogin);
      if (logins.length === 0) return socket.emit('myChats', []);
      // Fetch all user info in one query
      var users = await pool.query('SELECT login, nickname, avatar, username, vip_emoji, vip_until, verified FROM users WHERE login = ANY($1)', [logins]);
      // Fetch last messages for all chats in one query using DISTINCT ON
      var lastMsgs = await pool.query(
        'SELECT DISTINCT ON (LEAST(from_login,to_login), GREATEST(from_login,to_login)) from_login, to_login, text, type, timestamp FROM private_messages WHERE (from_login=$1 OR to_login=$1) AND from_login!=to_login ORDER BY LEAST(from_login,to_login), GREATEST(from_login,to_login), timestamp DESC',
        [socket.userLogin]
      );
      // Fetch unread counts in one query
      var unreadRes = await pool.query(
        'SELECT from_login, COUNT(*) as c FROM private_messages WHERE to_login=$1 AND read=false GROUP BY from_login',
        [socket.userLogin]
      );
      var unreadMap = {};
      unreadRes.rows.forEach(r => { unreadMap[r.from_login] = parseInt(r.c); });
      var lastMsgMap = {};
      lastMsgs.rows.forEach(r => {
        var other = r.from_login === socket.userLogin ? r.to_login : r.from_login;
        lastMsgMap[other] = r;
      });
      var chats = users.rows.map(u => {
        var lastMsg = lastMsgMap[u.login] || null;
        if (lastMsg && lastMsg.text && lastMsg.text.startsWith('{')) {
          try { var parsed = JSON.parse(lastMsg.text); if (parsed.callType) lastMsg = Object.assign({}, lastMsg, {text: parsed.answered ? '📞 Звонок' : '📞 Пропущенный звонок'}); } catch(e) {}
        }
        return { login: u.login, nickname: u.nickname, username: u.username || null, avatar: u.avatar || null, lastMsg, unread: unreadMap[u.login] || 0, vip_emoji: (u.vip_until > Date.now() ? u.vip_emoji : null) || null, vip_until: u.vip_until || 0, verified: u.verified || false };
      });
      chats.sort(function(a,b) { return (b.lastMsg?b.lastMsg.timestamp:0) - (a.lastMsg?a.lastMsg.timestamp:0); });
      socket.emit('myChats', chats);
    } catch(e) { console.error(e); socket.emit('myChats', []); }
  });

  socket.on('getPrivateHistory', async (data) => {
    var otherLogin = typeof data === 'string' ? data : data.login;
    var token = (typeof data === 'object' && data.token !== undefined) ? data.token : -1; // -1 never matches client token
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1) ORDER BY timestamp ASC LIMIT 200', [socket.userLogin, otherLogin]);
      const now = Date.now();
      const unreadRes = await pool.query('SELECT id FROM private_messages WHERE from_login=$1 AND to_login=$2 AND read=false', [otherLogin, socket.userLogin]);
      await pool.query('UPDATE private_messages SET read=true, read_at=$1 WHERE from_login=$2 AND to_login=$3 AND read=false', [now, otherLogin, socket.userLogin]);
      socket.emit('privateHistory', { otherLogin, messages: res.rows.map(fixMsgImages), token });
      // Fix: immediately notify client to clear unread badge
      socket.emit('clearUnreadBadge', { login: otherLogin });
      if (unreadRes.rows.length > 0) {
        const readIds = unreadRes.rows.map(r => r.id);
        const sender = findSocketByLogin(otherLogin);
        if (sender) sender.emit('messagesRead', { byLogin: socket.userLogin, msgIds: readIds, readAt: now });
      }
    } catch(e) { console.error(e); }
  });

  socket.on('privateMessage', async (data) => {
    if (!checkRateLimit(ip, 'message')) return socket.emit('rateLimited', { msg: 'Слишком быстро! Помедленнее.' });
    if (!socket.userLogin) return;
    if (data && data.text) data.text = sanitize(data.text, 4000);
    if (!socket.userLogin) return;
    try {
      const u = await pool.query('SELECT muted_until FROM users WHERE login=$1', [socket.userLogin]);
      if (u.rows.length > 0 && u.rows[0].muted_until > Date.now()) return socket.emit('chatError', 'Вы замучены');
    } catch(e) {}
    let vipEmojiPM = null;
    try { const ve = await pool.query('SELECT vip_emoji, vip_until FROM users WHERE login=$1', [socket.userLogin]); if (ve.rows[0] && ve.rows[0].vip_until > Date.now()) vipEmojiPM = ve.rows[0].vip_emoji || null; } catch(e) {}
    const msg = { from_login: socket.userLogin, vip_emoji: vipEmojiPM, to_login: data.toLogin, from_nickname: socket.username, text: data.text||'', image: serializeImage(data.image)||null, voice: data.voice||null, type: data.type||'text', timestamp: Date.now(), reply_to_id: data.reply_to_id||null, reply_to_text: data.reply_to_text||null, reply_to_user: data.reply_to_user||null, file_url: data.file_url||null, file_name: data.file_name||null, file_size: data.file_size||null };
    try {
      const encPmText = encryptText(msg.text);
      const encPmReply = encryptText(msg.reply_to_text);
      const res = await pool.query('INSERT INTO private_messages (from_login,to_login,from_nickname,text,image,voice,type,timestamp,reply_to_id,reply_to_text,reply_to_user,file_url,file_name,file_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id',
        [msg.from_login, msg.to_login, msg.from_nickname, encPmText, msg.image, msg.voice, msg.type, msg.timestamp, msg.reply_to_id, encPmReply, msg.reply_to_user, msg.file_url, msg.file_name, msg.file_size]);
      msg.id = res.rows[0].id;
      const msgToSend = fixMsgImages({...msg});
      // If sending to self (Saved Messages) — emit only once
      if (data.toLogin === socket.userLogin) {
        socket.emit('newPrivateMessage', msgToSend);
      } else {
        socket.emit('newPrivateMessage', msgToSend);
        var target = findSocketByLogin(data.toLogin);
        if (target) { target.emit('newPrivateMessage', msgToSend); target.emit('unreadNotification', { from: socket.userLogin, nickname: socket.username }); }
      }
      // Detect @mentions in PM text
      if (msg.text) {
        try {
          const pmMentionMatches = msg.text.match(/@([a-zA-Zа-яА-ЯёЁ0-9_]+)/g);
          if (pmMentionMatches) {
            const pmMentionNames = pmMentionMatches.map(m => m.slice(1).toLowerCase());
            const allPmUsers = await pool.query('SELECT login, nickname, username FROM users WHERE login != $1', [socket.userLogin]);
            allPmUsers.rows.forEach(u => {
              if (pmMentionNames.includes(u.nickname.toLowerCase()) || pmMentionNames.includes(u.login.toLowerCase()) || (u.username && pmMentionNames.includes(u.username.toLowerCase()))) {
                const ts = findSocketByLogin(u.login);
                if (ts) ts.emit('mentionReceived', { from: socket.username, text: msg.text, chatType: 'pm', chatId: socket.userLogin, msgId: msg.id, ts: msg.timestamp });
              }
            });
          }
        } catch(e) {}
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

  socket.on('openRoom', async (data) => {
    if (!socket.userLogin) return;
    var roomId = typeof data === 'object' ? Number(data.roomId) : Number(data);
    var token = typeof data === 'object' ? (data.token || null) : null;
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0) return socket.emit('chatError', 'Вы не участник этой комнаты');
      var room = await pool.query('SELECT * FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0) return socket.emit('chatError', 'Комната не найдена');
      var msgs = await pool.query('SELECT * FROM room_messages WHERE room_id=$1 ORDER BY timestamp ASC LIMIT 200', [roomId]);
      var members = await pool.query('SELECT rm.user_login, rm.role, u.nickname FROM room_members rm JOIN users u ON rm.user_login = u.login WHERE rm.room_id=$1 ORDER BY rm.role, u.nickname', [roomId]);
      socket.join('room_' + roomId);
      socket.emit('roomData', { room: room.rows[0], myRole: member.rows[0].role, messages: msgs.rows.map(fixMsgImages), members: members.rows, token });
    } catch(e) { console.error('openRoom error:', e); }
  });

  socket.on('roomMessage', async (data) => {
    if (!checkRateLimit(ip, 'message')) return socket.emit('rateLimited', { msg: 'Слишком быстро! Помедленнее.' });
    if (!socket.userLogin) return;
    if (data && data.text) data.text = sanitize(data.text, 4000);
    if (!socket.userLogin || !data.roomId) return;
    var roomId = Number(data.roomId);
    try {
      var member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (member.rows.length === 0) return;
      var room = await pool.query('SELECT type FROM rooms WHERE id=$1', [roomId]);
      if (room.rows.length === 0) return;
      if (room.rows[0].type === 'channel' && member.rows[0].role !== 'admin') return socket.emit('chatError', 'В канале писать может только админ');
      let vipEmojiR = null;
      try { const ve = await pool.query('SELECT vip_emoji, vip_until FROM users WHERE login=$1', [socket.userLogin]); if (ve.rows[0] && ve.rows[0].vip_until > Date.now()) vipEmojiR = ve.rows[0].vip_emoji || null; } catch(e) {}
      var msg = { room_id: roomId, user_login: socket.userLogin, vip_emoji: vipEmojiR, username: socket.username, text: data.text||'', image: serializeImage(data.image)||null, voice: data.voice||null, type: data.type||'text', timestamp: Date.now(), reply_to_id: data.reply_to_id||null, reply_to_text: data.reply_to_text||null, reply_to_user: data.reply_to_user||null, file_url: data.file_url||null, file_name: data.file_name||null, file_size: data.file_size||null };
      if (data.text) data.text = encryptText(data.text);
      if (data.reply_to_text) data.reply_to_text = encryptText(data.reply_to_text);
      var res = await pool.query('INSERT INTO room_messages (room_id, user_login, username, text, image, voice, type, timestamp, reply_to_id, reply_to_text, reply_to_user, file_url, file_name, file_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id',
        [msg.room_id, msg.user_login, msg.username, msg.text, msg.image, msg.voice, msg.type, msg.timestamp, msg.reply_to_id, msg.reply_to_text, msg.reply_to_user, msg.file_url, msg.file_name, msg.file_size]);
      msg.id = res.rows[0].id;
      io.to('room_' + roomId).emit('roomNewMessage', fixMsgImages({...msg}));
      // Detect @mentions in room text
      if (msg.text) {
        try {
          const roomMentionMatches = msg.text.match(/@([a-zA-Zа-яА-ЯёЁ0-9_]+)/g);
          if (roomMentionMatches) {
            const roomMentionNames = roomMentionMatches.map(m => m.slice(1).toLowerCase());
            const roomUsers = await pool.query('SELECT u.login, u.nickname, u.username FROM room_members rm JOIN users u ON rm.user_login=u.login WHERE rm.room_id=$1 AND u.login != $2', [roomId, socket.userLogin]);
            const roomInfo = await pool.query('SELECT name FROM rooms WHERE id=$1', [roomId]);
            const roomName = roomInfo.rows[0] ? roomInfo.rows[0].name : 'Группа';
            roomUsers.rows.forEach(u => {
              if (roomMentionNames.includes(u.nickname.toLowerCase()) || roomMentionNames.includes(u.login.toLowerCase()) || (u.username && roomMentionNames.includes(u.username.toLowerCase()))) {
                const ts = findSocketByLogin(u.login);
                if (ts) ts.emit('mentionReceived', { from: socket.username, text: msg.text, chatType: 'room', chatId: String(roomId), chatName: roomName, msgId: msg.id, ts: msg.timestamp });
              }
            });
          }
        } catch(e) {}
      }
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
    try { const res = await pool.query('SELECT id,login,nickname,banned,muted_until,role,vip_until,vip_emoji,verified FROM users ORDER BY id'); socket.emit('adminUsers', res.rows); } catch(e) {}
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

  socket.on('adminUnmuteUser', async (login) => {
    if (!isAdmin(socket)) return;
    try { await pool.query('UPDATE users SET muted_until=0 WHERE login=$1', [login]); socket.emit('adminDone', 'Мут снят: ' + login); } catch(e) {}
  });

  socket.on('adminRemoveVip', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET vip_until=0, vip_emoji=NULL WHERE login=$1', [login]);
      socket.emit('adminDone', 'VIP снят: ' + login);
      io.emit('userVipUpdated', { login, vip_until: 0, vip_emoji: null });
    } catch(e) {}
  });

  socket.on('adminRemoveVerify', async (login) => {
    if (!isAdmin(socket)) return;
    try {
      await pool.query('UPDATE users SET verified=false WHERE login=$1', [login]);
      socket.emit('adminDone', 'Верификация снята: ' + login);
      io.emit('userVerified', { login, verified: false });
    } catch(e) {}
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

  // === VIP SYSTEM ===
  socket.on('adminGenerateVipCode', async ({ days }) => {
    if (!isAdmin(socket)) return;
    const crypto = require('crypto');
    const code = 'VIP-' + crypto.randomBytes(6).toString('hex').toUpperCase();
    const d = parseInt(days) || 30;
    try {
      await pool.query('INSERT INTO vip_codes (code, duration_days, created_at) VALUES ($1,$2,$3)', [code, d, Date.now()]);
      socket.emit('adminVipCodeCreated', { code, days: d });
    } catch(e) { socket.emit('adminDone', 'Ошибка: ' + e.message); }
  });

  socket.on('adminGetVipCodes', async () => {
    if (!isAdmin(socket)) return;
    try {
      const res = await pool.query('SELECT * FROM vip_codes ORDER BY id DESC LIMIT 50');
      socket.emit('adminVipCodes', res.rows);
    } catch(e) {}
  });

  socket.on('activateVip', async ({ code }) => {
    if (!socket.userLogin) return;
    try {
      const cleanCode = (code || '').trim().toUpperCase();
      if (!cleanCode) { socket.emit('vipActivateResult', { ok: false, msg: 'Введи код' }); return; }
      const res = await pool.query('SELECT * FROM vip_codes WHERE UPPER(code)=$1 AND used=false', [cleanCode]);
      if (!res.rows.length) { socket.emit('vipActivateResult', { ok: false, msg: 'Неверный или использованный код' }); return; }
      const row = res.rows[0];
      // If user already has VIP, extend it
      const curUser = await pool.query('SELECT vip_until FROM users WHERE login=$1', [socket.userLogin]);
      const curUntil = curUser.rows[0]?.vip_until || 0;
      const base = curUntil > Date.now() ? curUntil : Date.now();
      const until = base + row.duration_days * 86400000;
      await pool.query('UPDATE vip_codes SET used=true, used_by=$1 WHERE id=$2', [socket.userLogin, row.id]);
      await pool.query('UPDATE users SET vip_until=$1 WHERE login=$2', [until, socket.userLogin]);
      socket.emit('vipActivateResult', { ok: true, until, days: row.duration_days });
      // Broadcast VIP status update
      io.emit('userVipUpdated', { login: socket.userLogin, vip_until: until, vip_emoji: curUser.rows[0]?.vip_emoji || null });
    } catch(e) { console.error('activateVip error:', e); socket.emit('vipActivateResult', { ok: false, msg: 'Ошибка сервера: ' + e.message }); }
  });

  socket.on('setVipEmoji', async ({ emoji }) => {
    if (!socket.userLogin) return;
    // check user has VIP
    const u = await pool.query('SELECT vip_until FROM users WHERE login=$1', [socket.userLogin]);
    if (!u.rows.length || (u.rows[0].vip_until || 0) < Date.now()) {
      socket.emit('vipEmojiResult', { ok: false, msg: 'Нужен VIP для установки смайлика' }); return;
    }
    await pool.query('UPDATE users SET vip_emoji=$1 WHERE login=$2', [emoji || null, socket.userLogin]);
    socket.emit('vipEmojiResult', { ok: true, emoji });
    // broadcast updated profile to all (so others see the emoji)
    io.emit('userVipUpdated', { login: socket.userLogin, vip_until: u.rows[0].vip_until, vip_emoji: emoji || null });
  });

  // === ONLINE STATUS & LAST SEEN ===
  socket.on('setHideOnline', async ({ hide }) => {
    if (!socket.userLogin) return;
    await pool.query('UPDATE users SET hide_online=$1 WHERE login=$2', [!!hide, socket.userLogin]);
    socket.emit('hideOnlineUpdated', { hide: !!hide });
  });

  socket.on('getLastSeen', async ({ login }) => {
    if (!socket.userLogin) return;
    const r = await pool.query('SELECT last_seen, hide_online FROM users WHERE login=$1', [login]);
    if (!r.rows.length) return;
    const row = r.rows[0];
    // If user hides online - send 'recently' flag
    const isOnline = [...onlineUsers.values()].some(u => u.login === login);
    if (row.hide_online) {
      socket.emit('lastSeenResult', { login, hidden: true, isOnline: false });
    } else {
      socket.emit('lastSeenResult', { login, hidden: false, isOnline, last_seen: row.last_seen || 0 });
    }
  });

  // === READ RECEIPTS ===
  socket.on('markRead', async ({ msgIds, fromLogin }) => {
    if (!socket.userLogin || !msgIds || !msgIds.length) return;
    const now = Date.now();
    await pool.query(
      'UPDATE private_messages SET read=true, read_at=$1 WHERE id=ANY($2) AND to_login=$3',
      [now, msgIds, socket.userLogin]
    );
    // Notify sender that their messages were read
    const target = findSocketByLogin(fromLogin);
    if (target) target.emit('messagesRead', { byLogin: socket.userLogin, msgIds, readAt: now });
  });

  // === PINNED CHATS ===
  socket.on('getPinnedChats', async () => {
    if (!socket.userLogin) return;
    const r = await pool.query('SELECT * FROM pinned_chats WHERE user_login=$1 ORDER BY pinned_at DESC', [socket.userLogin]);
    socket.emit('pinnedChats', r.rows);
  });

  socket.on('pinChat', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    await pool.query('INSERT INTO pinned_chats (user_login,chat_type,chat_id,pinned_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING',
      [socket.userLogin, chatType, String(chatId), Date.now()]);
    socket.emit('pinChatResult', { ok: true, chatType, chatId });
  });

  socket.on('unpinChat', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    await pool.query('DELETE FROM pinned_chats WHERE user_login=$1 AND chat_type=$2 AND chat_id=$3',
      [socket.userLogin, chatType, String(chatId)]);
    socket.emit('unpinChatResult', { ok: true, chatType, chatId });
  });

  // === REACTIONS ===
  socket.on('addReaction', async ({ msgType, msgId, emoji }) => {
    if (!socket.userLogin) return;
    try {
      await pool.query('INSERT INTO reactions (msg_type, msg_id, user_login, emoji) VALUES ($1,$2,$3,$4) ON CONFLICT (msg_type, msg_id, user_login) DO UPDATE SET emoji=$4',
        [msgType, msgId, socket.userLogin, emoji]);
      const res = await pool.query('SELECT emoji, COUNT(*)::int as count FROM reactions WHERE msg_type=$1 AND msg_id=$2 GROUP BY emoji', [msgType, msgId]);
      const userRes = await pool.query('SELECT user_login FROM reactions WHERE msg_type=$1 AND msg_id=$2 AND emoji=$3', [msgType, msgId, emoji]);
      const payload = { msgType, msgId, reactions: res.rows };
      if (msgType === 'general') io.emit('reactionUpdated', payload);
      else if (msgType === 'pm') {
        // find both users involved
        const msg = await pool.query('SELECT from_login, to_login FROM private_messages WHERE id=$1', [msgId]);
        if (msg.rows.length > 0) {
          socket.emit('reactionUpdated', payload);
          const other = msg.rows[0].from_login === socket.userLogin ? msg.rows[0].to_login : msg.rows[0].from_login;
          const target = findSocketByLogin(other);
          if (target) target.emit('reactionUpdated', payload);
        }
      } else if (msgType === 'room') {
        const msg = await pool.query('SELECT room_id FROM room_messages WHERE id=$1', [msgId]);
        if (msg.rows.length > 0) io.to('room_' + msg.rows[0].room_id).emit('reactionUpdated', payload);
      }
    } catch(e) { console.error(e); }
  });

  socket.on('getReactions', async ({ msgType, msgId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT emoji, COUNT(*)::int as count FROM reactions WHERE msg_type=$1 AND msg_id=$2 GROUP BY emoji', [msgType, msgId]);
      const mine = await pool.query('SELECT emoji FROM reactions WHERE msg_type=$1 AND msg_id=$2 AND user_login=$3', [msgType, msgId, socket.userLogin]);
      socket.emit('reactionsData', { msgType, msgId, reactions: res.rows, myEmoji: mine.rows[0]?.emoji || null });
    } catch(e) {}
  });

  // === VERIFICATION ===
  socket.on('adminGenerateVerifyCode', async () => {
    if (!isAdmin(socket)) return;
    const code = 'VRF-' + crypto.randomBytes(6).toString('hex').toUpperCase();
    try {
      await pool.query('INSERT INTO verify_codes (code, created_at) VALUES ($1,$2)', [code, Date.now()]);
      socket.emit('adminVerifyCodeCreated', { code });
    } catch(e) { socket.emit('adminDone', 'Ошибка: ' + e.message); }
  });

  socket.on('adminGetVerifyCodes', async () => {
    if (!isAdmin(socket)) return;
    try {
      const res = await pool.query('SELECT * FROM verify_codes ORDER BY id DESC LIMIT 50');
      socket.emit('adminVerifyCodes', res.rows);
    } catch(e) {}
  });

  socket.on('activateVerify', async ({ code }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM verify_codes WHERE code=$1 AND used=false', [code.trim().toUpperCase()]);
      if (!res.rows.length) { socket.emit('verifyActivateResult', { ok: false, msg: 'Неверный или использованный код' }); return; }
      await pool.query('UPDATE verify_codes SET used=true, used_by=$1 WHERE id=$2', [socket.userLogin, res.rows[0].id]);
      await pool.query('UPDATE users SET verified=true WHERE login=$1', [socket.userLogin]);
      socket.emit('verifyActivateResult', { ok: true });
      io.emit('userVerified', { login: socket.userLogin });
    } catch(e) { socket.emit('verifyActivateResult', { ok: false, msg: 'Ошибка сервера' }); }
  });

  // === MUTE CHAT ===
  socket.on('muteChat', async ({ chatType, chatId, hours }) => {
    if (!socket.userLogin) return;
    const until = Date.now() + (hours || 1) * 3600000;
    await pool.query('INSERT INTO muted_chats (user_login,chat_type,chat_id,muted_until) VALUES ($1,$2,$3,$4) ON CONFLICT (user_login,chat_type,chat_id) DO UPDATE SET muted_until=$4',
      [socket.userLogin, chatType, String(chatId), until]);
    socket.emit('muteChatResult', { ok: true, chatType, chatId, until });
  });

  socket.on('unmuteChat', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    await pool.query('DELETE FROM muted_chats WHERE user_login=$1 AND chat_type=$2 AND chat_id=$3', [socket.userLogin, chatType, String(chatId)]);
    socket.emit('muteChatResult', { ok: true, chatType, chatId, until: 0 });
  });

  socket.on('getMutedChats', async () => {
    if (!socket.userLogin) return;
    const res = await pool.query('SELECT * FROM muted_chats WHERE user_login=$1 AND muted_until>$2', [socket.userLogin, Date.now()]);
    socket.emit('mutedChats', res.rows);
  });

  // === EDIT MESSAGE ===
  socket.on('editMessage', async ({ msgType, msgId, newText }) => {
    if (!socket.userLogin || !newText || !newText.trim()) return;
    try {
      const cleanText = newText.trim();
      const encText = encryptText(cleanText); // encrypt before storing
      if (msgType === 'general') {
        const res = await pool.query('UPDATE messages SET text=$1, edited=true WHERE id=$2 AND (username=$3 OR $4=true) RETURNING id', [encText, msgId, socket.username, isAdmin(socket)]);
        if (res.rows.length) io.emit('messageEdited', { msgType, msgId, newText: cleanText });
      } else if (msgType === 'pm') {
        const res = await pool.query('UPDATE private_messages SET text=$1, edited=true WHERE id=$2 AND from_login=$3 RETURNING from_login, to_login', [encText, msgId, socket.userLogin]);
        if (res.rows.length) {
          const other = res.rows[0].to_login;
          const payload = { msgType, msgId, newText: cleanText };
          socket.emit('messageEdited', payload);
          const t = findSocketByLogin(other); if (t) t.emit('messageEdited', payload);
        }
      } else if (msgType === 'room') {
        const msg = await pool.query('SELECT room_id FROM room_messages WHERE id=$1', [msgId]);
        if (!msg.rows.length) return;
        const roomId = msg.rows[0].room_id;
        const res = await pool.query('UPDATE room_messages SET text=$1, edited=true WHERE id=$2 AND (user_login=$3 OR $4=true) RETURNING id', [encText, msgId, socket.userLogin, isAdmin(socket)]);
        if (res.rows.length) io.to('room_' + roomId).emit('messageEdited', { msgType, msgId, newText: cleanText });
      }
    } catch(e) { console.error(e); }
  });

  // === FORWARD MESSAGE ===
  socket.on('forwardMessage', async ({ msgType, msgId, toType, toId }) => {
    if (!socket.userLogin) return;
    try {
      let origText = '', origImage = null, origVoice = null, origMsgType = 'text';
      if (msgType === 'general') {
        const r = await pool.query('SELECT * FROM messages WHERE id=$1', [msgId]);
        if (!r.rows.length) return;
        const m = r.rows[0]; origText = m.text; origImage = m.image; origVoice = m.voice; origMsgType = m.type;
      } else if (msgType === 'pm') {
        const r = await pool.query('SELECT * FROM private_messages WHERE id=$1 AND (from_login=$2 OR to_login=$2)', [msgId, socket.userLogin]);
        if (!r.rows.length) return;
        const m = r.rows[0]; origText = m.text; origImage = m.image; origVoice = m.voice; origMsgType = m.type;
      } else if (msgType === 'room') {
        const r = await pool.query('SELECT * FROM room_messages WHERE id=$1', [msgId]);
        if (!r.rows.length) return;
        const m = r.rows[0]; origText = m.text; origImage = m.image; origVoice = m.voice; origMsgType = m.type;
      }
      const fwdNick = socket.username;
      let vipEmojiF = null;
      try { const ve = await pool.query('SELECT vip_emoji, vip_until FROM users WHERE login=$1', [socket.userLogin]); if (ve.rows[0] && ve.rows[0].vip_until > Date.now()) vipEmojiF = ve.rows[0].vip_emoji || null; } catch(e) {}

      if (toType === 'general') {
        const res = await pool.query('INSERT INTO messages (username,user_login,text,image,voice,type,timestamp,fwd_from_nick) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id',
          [socket.username, socket.userLogin, origText, origImage, origVoice, origMsgType, Date.now(), fwdNick]);
        const msg = { id: res.rows[0].id, username: socket.username, user_login: socket.userLogin, vip_emoji: vipEmojiF, text: origText, image: origImage, voice: origVoice, type: origMsgType, timestamp: Date.now(), fwd_from_nick: fwdNick };
        io.emit('chatMessage', fixMsgImages({...msg}));
      } else if (toType === 'pm') {
        const res = await pool.query('INSERT INTO private_messages (from_login,to_login,from_nickname,text,image,voice,type,timestamp,fwd_from_nick) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id',
          [socket.userLogin, toId, socket.username, origText, origImage, origVoice, origMsgType, Date.now(), fwdNick]);
        const msg = { id: res.rows[0].id, from_login: socket.userLogin, to_login: toId, from_nickname: socket.username, vip_emoji: vipEmojiF, text: origText, image: origImage, voice: origVoice, type: origMsgType, timestamp: Date.now(), fwd_from_nick: fwdNick };
        const msgToSend = fixMsgImages({...msg});
        socket.emit('newPrivateMessage', msgToSend);
        const t = findSocketByLogin(toId); if (t) { t.emit('newPrivateMessage', msgToSend); t.emit('unreadNotification', { from: socket.userLogin, nickname: socket.username }); }
      } else if (toType === 'room') {
        const roomId = Number(toId);
        const member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
        if (!member.rows.length) return;
        const res = await pool.query('INSERT INTO room_messages (room_id,user_login,username,text,image,voice,type,timestamp,fwd_from_nick) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id',
          [roomId, socket.userLogin, socket.username, origText, origImage, origVoice, origMsgType, Date.now(), fwdNick]);
        const msg = { id: res.rows[0].id, room_id: roomId, user_login: socket.userLogin, username: socket.username, vip_emoji: vipEmojiF, text: origText, image: origImage, voice: origVoice, type: origMsgType, timestamp: Date.now(), fwd_from_nick: fwdNick };
        io.to('room_' + roomId).emit('roomNewMessage', fixMsgImages({...msg}));
      }
      socket.emit('forwardDone', { ok: true });
    } catch(e) { console.error(e); socket.emit('forwardDone', { ok: false }); }
  });

  // === SAVED MESSAGES (send to self) ===
  // Handled via normal privateMessage to own login on frontend

  // === GROUP READ RECEIPTS ===
  socket.on('markRoomRead', async ({ roomId, msgIds }) => {
    if (!socket.userLogin || !msgIds || !msgIds.length) return;
    const now = Date.now();
    for (const mid of msgIds) {
      try {
        await pool.query('INSERT INTO room_message_reads (msg_id,user_login,read_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [mid, socket.userLogin, now]);
      } catch(e) {}
    }
  });

  socket.on('getRoomMsgReaders', async ({ msgId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT r.user_login, u.nickname FROM room_message_reads r JOIN users u ON r.user_login=u.login WHERE r.msg_id=$1', [msgId]);
      socket.emit('roomMsgReaders', { msgId, readers: res.rows });
    } catch(e) {}
  });

  // === STORIES ===
  socket.on('addStory', async ({ mediaUrl, mediaType, text }) => {
    if (!socket.userLogin) return;
    if (!mediaUrl) return socket.emit('storyResult', { ok: false, msg: 'Нет медиа' });
    // Limit story media size (~40MB base64 = ~30MB file)
    if (mediaUrl.length > 42 * 1024 * 1024) return socket.emit('storyResult', { ok: false, msg: 'Файл слишком большой (макс. 30 МБ)' });
    const ts = Date.now();
    const expires = ts + 86400000; // 24h
    try {
      const res = await pool.query('INSERT INTO stories (user_login,user_nickname,media_url,media_type,text,timestamp,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id',
        [socket.userLogin, socket.username, mediaUrl, mediaType||'image', text||null, ts, expires]);
      const story = { id: res.rows[0].id, user_login: socket.userLogin, user_nickname: socket.username, media_url: mediaUrl, media_type: mediaType||'image', text: text||null, timestamp: ts, expires_at: expires, views: 0 };
      io.emit('newStory', story);
      socket.emit('storyResult', { ok: true, story });
    } catch(e) { socket.emit('storyResult', { ok: false, msg: e.message }); }
  });

  socket.on('getStories', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT s.*, (SELECT COUNT(*) FROM story_views WHERE story_id=s.id) as views, (SELECT COUNT(*) FROM story_views WHERE story_id=s.id AND viewer_login=$1) as viewed FROM stories s WHERE s.expires_at>$2 ORDER BY s.timestamp DESC',
        [socket.userLogin, Date.now()]);
      socket.emit('storiesData', res.rows);
    } catch(e) { socket.emit('storiesData', []); }
  });

  socket.on('viewStory', async ({ storyId }) => {
    if (!socket.userLogin) return;
    try {
      await pool.query('INSERT INTO story_views (story_id,viewer_login,viewed_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [storyId, socket.userLogin, Date.now()]);
      // Get story owner
      const s = await pool.query('SELECT user_login FROM stories WHERE id=$1', [storyId]);
      if (s.rows.length) {
        const owner = findSocketByLogin(s.rows[0].user_login);
        if (owner) owner.emit('storyViewed', { storyId, viewerLogin: socket.userLogin, viewerNick: socket.username });
      }
    } catch(e) {}
  });

  socket.on('deleteStory', async ({ storyId }) => {
    if (!socket.userLogin) return;
    const res = await pool.query('SELECT user_login FROM stories WHERE id=$1', [storyId]);
    if (!res.rows.length) return;
    if (res.rows[0].user_login !== socket.userLogin && !isAdmin(socket)) return;
    await pool.query('DELETE FROM stories WHERE id=$1', [storyId]);
    io.emit('storyDeleted', { storyId });
  });

  socket.on('getStoryViewers', async ({ storyId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT sv.viewer_login, u.nickname FROM story_views sv JOIN users u ON sv.viewer_login=u.login WHERE sv.story_id=$1 ORDER BY sv.viewed_at DESC', [storyId]);
      socket.emit('storyViewers', { storyId, viewers: res.rows });
    } catch(e) {}
  });

  // === PER-DIALOG BACKGROUND ===
  socket.on('setDialogBg', async ({ chatType, chatId, bgId, bgData }) => {
    if (!socket.userLogin) return;
    try {
      await pool.query('INSERT INTO dialog_bg (user_login,chat_type,chat_id,bg_id,bg_data) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (user_login,chat_type,chat_id) DO UPDATE SET bg_id=$4, bg_data=$5',
        [socket.userLogin, chatType, String(chatId), bgId || 'none', bgData || null]);
      socket.emit('dialogBgResult', { ok: true, chatType, chatId, bgId, bgData });
    } catch(e) { socket.emit('dialogBgResult', { ok: false }); }
  });

  socket.on('getDialogBg', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT bg_id, bg_data FROM dialog_bg WHERE user_login=$1 AND chat_type=$2 AND chat_id=$3', [socket.userLogin, chatType, String(chatId)]);
      if (res.rows.length) socket.emit('dialogBgData', { chatType, chatId, bgId: res.rows[0].bg_id, bgData: res.rows[0].bg_data });
      else socket.emit('dialogBgData', { chatType, chatId, bgId: 'none', bgData: null });
    } catch(e) {}
  });


  // === BLOCK / UNBLOCK USER ===
  socket.on('blockUser', async ({ login }) => {
    if (!socket.userLogin || !login || login === socket.userLogin) return;
    try {
      await pool.query(
        'INSERT INTO blocked_users (blocker_login, blocked_login, created_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',
        [socket.userLogin, login, Date.now()]
      );
      socket.emit('blockResult', { ok: true, login, action: 'blocked' });
    } catch(e) { socket.emit('blockResult', { ok: false, msg: e.message }); }
  });

  socket.on('unblockUser', async ({ login }) => {
    if (!socket.userLogin || !login) return;
    try {
      await pool.query('DELETE FROM blocked_users WHERE blocker_login=$1 AND blocked_login=$2', [socket.userLogin, login]);
      socket.emit('blockResult', { ok: true, login, action: 'unblocked' });
    } catch(e) { socket.emit('blockResult', { ok: false, msg: e.message }); }
  });

  socket.on('getBlockedUsers', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query(
        'SELECT bu.blocked_login, u.nickname FROM blocked_users bu LEFT JOIN users u ON bu.blocked_login=u.login WHERE bu.blocker_login=$1',
        [socket.userLogin]
      );
      socket.emit('blockedUsers', res.rows);
    } catch(e) { socket.emit('blockedUsers', []); }
  });

  socket.on('checkBlocked', async ({ login }) => {
    if (!socket.userLogin || !login) return;
    try {
      const r1 = await pool.query('SELECT id FROM blocked_users WHERE blocker_login=$1 AND blocked_login=$2', [socket.userLogin, login]);
      const r2 = await pool.query('SELECT id FROM blocked_users WHERE blocker_login=$1 AND blocked_login=$2', [login, socket.userLogin]);
      socket.emit('blockStatus', { login, iBlockedThem: r1.rows.length > 0, theyBlockedMe: r2.rows.length > 0 });
    } catch(e) {}
  });

  // === DELETE PRIVATE CHAT HISTORY ===
  socket.on('deleteChatHistory', async ({ login, forBoth }) => {
    if (!socket.userLogin || !login) return;
    try {
      if (forBoth) {
        // Delete messages for both sides
        await pool.query(
          'DELETE FROM private_messages WHERE (from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1)',
          [socket.userLogin, login]
        );
        // Notify the other user too
        const target = findSocketByLogin(login);
        if (target) target.emit('chatHistoryDeleted', { login: socket.userLogin });
        socket.emit('chatHistoryDeleted', { login });
      } else {
        // Only delete from my side — mark messages as deleted for me
        // Simplest approach: delete where I am sender, clear read receipts visible to me
        await pool.query(
          'DELETE FROM private_messages WHERE from_login=$1 AND to_login=$2',
          [socket.userLogin, login]
        );
        // Also delete messages sent to me (so I don't see them)
        await pool.query(
          'DELETE FROM private_messages WHERE from_login=$2 AND to_login=$1',
          [socket.userLogin, login]
        );
        socket.emit('chatHistoryDeleted', { login });
      }
    } catch(e) { console.error('deleteChatHistory error:', e); socket.emit('chatError', 'Ошибка удаления истории'); }
  });


  // ── POLLS ───────────────────────────────────────────────
  socket.on('createPoll', async ({ chatType, chatId, question, options, multipleChoice, anonymous }) => {
    if (!socket.userLogin) return;
    if (!question || !options || options.length < 2 || options.length > 10) return socket.emit('chatError', 'Опрос: от 2 до 10 вариантов');
    try {
      const opts = options.map((text, i) => ({ id: i, text: String(text).slice(0, 100) }));
      const res = await pool.query(
        'INSERT INTO polls (chat_type,chat_id,creator_login,question,options,votes,created_at,multiple_choice,anonymous) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id',
        [chatType, String(chatId), socket.userLogin, String(question).slice(0, 300), JSON.stringify(opts), '{}', Date.now(), !!multipleChoice, !!anonymous]
      );
      const poll = { id: res.rows[0].id, chatType, chatId, question, options: opts, votes: {}, creatorLogin: socket.userLogin, creatorNick: socket.username, multipleChoice: !!multipleChoice, anonymous: !!anonymous, createdAt: Date.now() };
      // Broadcast to correct chat
      if (chatType === 'general') io.emit('newPoll', poll);
      else if (chatType === 'room') io.to('room_' + chatId).emit('newPoll', poll);
      else if (chatType === 'pm') {
        socket.emit('newPoll', poll);
        const other = findSocketByLogin(chatId);
        if (other) other.emit('newPoll', poll);
      }
    } catch(e) { console.error(e); }
  });

  socket.on('votePoll', async ({ pollId, optionId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM polls WHERE id=$1', [pollId]);
      if (!res.rows.length) return;
      const poll = res.rows[0];
      let votes = poll.votes || {};
      if (poll.multiple_choice) {
        if (!Array.isArray(votes[socket.userLogin])) votes[socket.userLogin] = [];
        const idx = votes[socket.userLogin].indexOf(optionId);
        if (idx === -1) votes[socket.userLogin].push(optionId);
        else votes[socket.userLogin].splice(idx, 1);
      } else {
        if (votes[socket.userLogin] === optionId) delete votes[socket.userLogin];
        else votes[socket.userLogin] = optionId;
      }
      await pool.query('UPDATE polls SET votes=$1 WHERE id=$2', [JSON.stringify(votes), pollId]);
      // Build results to send (anonymous = hide who voted)
      const result = { pollId, votes: poll.anonymous ? anonymizeVotes(votes) : votes };
      if (poll.chat_type === 'general') io.emit('pollVoteUpdate', result);
      else if (poll.chat_type === 'room') io.to('room_' + poll.chat_id).emit('pollVoteUpdate', result);
      else if (poll.chat_type === 'pm') {
        socket.emit('pollVoteUpdate', result);
        const other = findSocketByLogin(poll.chat_id);
        if (other) other.emit('pollVoteUpdate', result);
      }
    } catch(e) { console.error(e); }
  });

  socket.on('getPolls', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM polls WHERE chat_type=$1 AND chat_id=$2 ORDER BY created_at DESC LIMIT 20', [chatType, String(chatId)]);
      socket.emit('pollsHistory', res.rows.map(p => ({
        id: p.id, question: p.question, options: p.options, votes: p.anonymous ? anonymizeVotes(p.votes) : p.votes,
        creatorLogin: p.creator_login, multipleChoice: p.multiple_choice, anonymous: p.anonymous, createdAt: p.created_at
      })));
    } catch(e) {}
  });

  socket.on('deletePoll', async ({ pollId }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT creator_login, chat_type, chat_id FROM polls WHERE id=$1', [pollId]);
      if (!res.rows.length) return;
      const p = res.rows[0];
      if (p.creator_login !== socket.userLogin && !isAdmin(socket)) return;
      await pool.query('DELETE FROM polls WHERE id=$1', [pollId]);
      const payload = { pollId };
      if (p.chat_type === 'general') io.emit('pollDeleted', payload);
      else if (p.chat_type === 'room') io.to('room_' + p.chat_id).emit('pollDeleted', payload);
      else { socket.emit('pollDeleted', payload); const other = findSocketByLogin(p.chat_id); if(other) other.emit('pollDeleted', payload); }
    } catch(e) {}
  });




  // ── AI BOT ───────────────────────────────────────────────
  socket.on('askAiBot', async ({ message, history }) => {
    if (!socket.userLogin) return;
    if (!message || !message.trim()) return;
    if (!checkRateLimit(ip, 'message')) return socket.emit('rateLimited', { msg: 'Слишком быстро!' });
    try {
      const reply = await askAiBot(sanitize(message, 1000), history || []);
      const ts = Date.now();
      const msg = { from_login: AI_BOT_LOGIN, to_login: socket.userLogin, from_nickname: AI_BOT_NICK, text: reply, type: 'text', timestamp: ts, id: Date.now() };
      // Save to DB
      const encReply = encryptText(reply);
      const encMsg = encryptText(message);
      const res = await pool.query('INSERT INTO private_messages (from_login,to_login,from_nickname,text,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
        [AI_BOT_LOGIN, socket.userLogin, AI_BOT_NICK, encReply, 'text', ts]);
      msg.id = res.rows[0].id;
      socket.emit('newPrivateMessage', msg);
      socket.emit('getMyChats');
    } catch(e) { console.error(e); socket.emit('chatError', 'Ошибка AI бота'); }
  });

  // ── SCHEDULED MESSAGES ───────────────────────────────────
  socket.on('scheduleMessage', async ({ chatType, chatId, text, sendAt }) => {
    if (!socket.userLogin || !text || !sendAt) return;
    if (sendAt <= Date.now()) return socket.emit('chatError', 'Время должно быть в будущем');
    if (sendAt - Date.now() > 365 * 24 * 3600 * 1000) return socket.emit('chatError', 'Максимум 1 год вперёд');
    try {
      const res = await pool.query(
        'INSERT INTO scheduled_messages (user_login,chat_type,chat_id,text,send_at,created_at) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
        [socket.userLogin, chatType, String(chatId), sanitize(text, 4000), sendAt, Date.now()]
      );
      socket.emit('scheduledMsgCreated', { id: res.rows[0].id, chatType, chatId, text, sendAt });
    } catch(e) { console.error(e); }
  });

  socket.on('getScheduledMessages', async () => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT * FROM scheduled_messages WHERE user_login=$1 AND sent=false ORDER BY send_at ASC', [socket.userLogin]);
      socket.emit('scheduledMessages', res.rows);
    } catch(e) {}
  });

  socket.on('cancelScheduledMessage', async ({ id }) => {
    if (!socket.userLogin) return;
    try {
      await pool.query('DELETE FROM scheduled_messages WHERE id=$1 AND user_login=$2', [id, socket.userLogin]);
      socket.emit('scheduledMsgCancelled', { id });
    } catch(e) {}
  });

  // ── SECRET CHAT TIMER ────────────────────────────────────
  socket.on('setSecretTimer', async ({ otherLogin, timerSeconds }) => {
    if (!socket.userLogin) return;
    const t = parseInt(timerSeconds) || 0;
    try {
      await pool.query(
        'INSERT INTO secret_chat_settings (user_login, other_login, timer_seconds) VALUES ($1,$2,$3) ON CONFLICT (user_login, other_login) DO UPDATE SET timer_seconds=$3',
        [socket.userLogin, otherLogin, t]
      );
      socket.emit('secretTimerSet', { otherLogin, timerSeconds: t });
      const other = findSocketByLogin(otherLogin);
      if (other) other.emit('secretTimerSet', { otherLogin: socket.userLogin, timerSeconds: t });
    } catch(e) { console.error(e); }
  });

  socket.on('getSecretTimer', async ({ otherLogin }) => {
    if (!socket.userLogin) return;
    try {
      const res = await pool.query('SELECT timer_seconds FROM secret_chat_settings WHERE user_login=$1 AND other_login=$2', [socket.userLogin, otherLogin]);
      socket.emit('secretTimerResult', { otherLogin, timerSeconds: res.rows[0]?.timer_seconds || 0 });
    } catch(e) {}
  });

  socket.on('disconnect', () => {
    if (socket.username) addLog('logout', socket.username, 'Logout', ip);
    if (socket.userLogin) {
      pool.query('UPDATE users SET last_seen=$1 WHERE login=$2', [Date.now(), socket.userLogin]).catch(()=>{});
    }
    onlineUsers.delete(socket.id); socketUsers.delete(socket.id);
    sendOnlineToAll();
  });

  // === ЗАКРЕП СООБЩЕНИЙ ===
  socket.on('pinMessage', async ({ chatType, chatId, msgId, msgText, msgUser }) => {
    if (!socket.userLogin) { console.log('[PIN] rejected: no userLogin'); return; }
    console.log('[PIN] pinMessage from', socket.userLogin, { chatType, chatId, msgId });
    try {
      if (chatType === 'room') {
        const member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [Number(chatId), socket.userLogin]);
        if (!member.rows.length || member.rows[0].role !== 'admin') return socket.emit('chatError', 'Только админ может закреплять сообщения');
      }
      await pool.query(
        'INSERT INTO pinned_messages (chat_type,chat_id,msg_id,msg_text,msg_user,pinned_by,pinned_at) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (chat_type,chat_id) DO UPDATE SET msg_id=$3,msg_text=$4,msg_user=$5,pinned_by=$6,pinned_at=$7',
        [chatType, String(chatId), msgId, msgText || '', msgUser || '', socket.userLogin, Date.now()]
      );
      const payload = { chatType, chatId, msgId, msgText: msgText || '', msgUser: msgUser || '' };
      if (chatType === 'general') io.emit('messagePinned', payload);
      else if (chatType === 'room') io.to('room_' + chatId).emit('messagePinned', payload);
      else {
        socket.emit('messagePinned', payload);
        const other = findSocketByLogin(chatId);
        if (other) other.emit('messagePinned', payload);
      }
    } catch(e) { console.error('pinMessage error:', e); }
  });

  socket.on('unpinMessage', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    try {
      if (chatType === 'room') {
        const member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [Number(chatId), socket.userLogin]);
        if (!member.rows.length || member.rows[0].role !== 'admin') return socket.emit('chatError', 'Только админ может откреплять');
      }
      await pool.query('DELETE FROM pinned_messages WHERE chat_type=$1 AND chat_id=$2', [chatType, String(chatId)]);
      const payload = { chatType, chatId };
      if (chatType === 'general') io.emit('messageUnpinned', payload);
      else if (chatType === 'room') io.to('room_' + chatId).emit('messageUnpinned', payload);
      else { socket.emit('messageUnpinned', payload); const other = findSocketByLogin(chatId); if(other) other.emit('messageUnpinned', payload); }
    } catch(e) {}
  });

  socket.on('getPinnedMessage', async ({ chatType, chatId }) => {
    if (!socket.userLogin) return;
    console.log('[PIN] getPinnedMessage', { chatType, chatId, user: socket.userLogin });
    try {
      const res = await pool.query('SELECT * FROM pinned_messages WHERE chat_type=$1 AND chat_id=$2', [chatType, String(chatId)]);
      console.log('[PIN] DB result:', res.rows[0] || 'none');
      if (res.rows.length) socket.emit('pinnedMessage', { chatType, chatId, ...res.rows[0] });
      else socket.emit('pinnedMessage', { chatType, chatId, msg_id: null });
    } catch(e) { console.error('[PIN] getPinnedMessage error:', e); }
  });

  // === АНАЛИТИКА ГРУПП ===
  socket.on('getRoomAnalytics', async ({ roomId }) => {
    if (!socket.userLogin) return;
    roomId = Number(roomId);
    try {
      const member = await pool.query('SELECT role FROM room_members WHERE room_id=$1 AND user_login=$2', [roomId, socket.userLogin]);
      if (!member.rows.length) return;
      const totalMsgs = await pool.query('SELECT COUNT(*)::int as c FROM room_messages WHERE room_id=$1', [roomId]);
      const totalMembers = await pool.query('SELECT COUNT(*)::int as c FROM room_members WHERE room_id=$1', [roomId]);
      const topUsers = await pool.query('SELECT username, user_login, COUNT(*)::int as msg_count FROM room_messages WHERE room_id=$1 GROUP BY username, user_login ORDER BY msg_count DESC LIMIT 10', [roomId]);
      const sevenDaysAgo = Date.now() - 7 * 24 * 3600 * 1000;
      const dailyMsgs = await pool.query("SELECT to_char(to_timestamp(timestamp/1000), 'DD.MM') as day, COUNT(*)::int as count FROM room_messages WHERE room_id=$1 AND timestamp>$2 GROUP BY day ORDER BY day", [roomId, sevenDaysAgo]);
      socket.emit('roomAnalytics', { roomId, totalMessages: totalMsgs.rows[0].c, totalMembers: totalMembers.rows[0].c, topUsers: topUsers.rows, dailyMessages: dailyMsgs.rows });
    } catch(e) { console.error('getRoomAnalytics error:', e); }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
// ── SCHEDULED MESSAGES ──────────────────────────────────
async function initScheduledTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS scheduled_messages (
    id SERIAL PRIMARY KEY,
    user_login VARCHAR(50) NOT NULL,
    chat_type VARCHAR(10) NOT NULL,
    chat_id VARCHAR(100) NOT NULL,
    text TEXT,
    send_at BIGINT NOT NULL,
    sent BOOLEAN DEFAULT false,
    created_at BIGINT NOT NULL
  )`);
}
initScheduledTables(pool).catch(console.error);

// Check and send scheduled messages every 30 seconds
setInterval(async () => {
  try {
    const now = Date.now();
    const due = await pool.query('SELECT * FROM scheduled_messages WHERE sent=false AND send_at <= $1 LIMIT 50', [now]);
    for (const msg of due.rows) {
      await pool.query('UPDATE scheduled_messages SET sent=true WHERE id=$1', [msg.id]);
      const userSocket = findSocketByLogin(msg.user_login);
      const encText = encryptText(msg.text);
      const ts = Date.now();
      try {
        if (msg.chat_type === 'general') {
          const res = await pool.query('INSERT INTO messages (username,user_login,text,type,timestamp) VALUES ($1,$2,$3,$4,$5) RETURNING id',
            [msg.user_login, msg.user_login, encText, 'text', ts]);
          const outMsg = { id: res.rows[0].id, username: msg.user_login, user_login: msg.user_login, text: msg.text, type: 'text', timestamp: ts };
          io.emit('chatMessage', outMsg);
        } else if (msg.chat_type === 'pm') {
          const u = await pool.query('SELECT nickname FROM users WHERE login=$1', [msg.user_login]);
          const nick = u.rows[0]?.nickname || msg.user_login;
          const res = await pool.query('INSERT INTO private_messages (from_login,to_login,from_nickname,text,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
            [msg.user_login, msg.chat_id, nick, encText, 'text', ts]);
          const outMsg = { id: res.rows[0].id, from_login: msg.user_login, to_login: msg.chat_id, from_nickname: nick, text: msg.text, type: 'text', timestamp: ts };
          if (userSocket) userSocket.emit('newPrivateMessage', outMsg);
          const target = findSocketByLogin(msg.chat_id);
          if (target && msg.chat_id !== msg.user_login) target.emit('newPrivateMessage', outMsg);
        } else if (msg.chat_type === 'room') {
          const u = await pool.query('SELECT nickname FROM users WHERE login=$1', [msg.user_login]);
          const nick = u.rows[0]?.nickname || msg.user_login;
          const res = await pool.query('INSERT INTO room_messages (room_id,user_login,username,text,type,timestamp) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id',
            [parseInt(msg.chat_id), msg.user_login, nick, encText, 'text', ts]);
          const outMsg = { id: res.rows[0].id, room_id: parseInt(msg.chat_id), user_login: msg.user_login, username: nick, text: msg.text, type: 'text', timestamp: ts };
          io.to('room_' + msg.chat_id).emit('roomNewMessage', outMsg);
        }
      } catch(e) { console.error('scheduled send error:', e); }
      if (userSocket) userSocket.emit('scheduledMsgSent', { id: msg.id });
    }
  } catch(e) { console.error('scheduler error:', e); }
}, 30_000);

// ── AI BOT INTEGRATION ──────────────────────────────────
// Set AI_BOT_KEY env variable to your OpenAI/OpenRouter API key
const AI_BOT_KEY = process.env.AI_BOT_KEY || null;
const AI_BOT_LOGIN = '_ai_bot';
const AI_BOT_NICK = '🤖 AI Ассистент';

async function initAiBot(pool) {
  try {
    const exists = await pool.query('SELECT login FROM users WHERE login=$1', [AI_BOT_LOGIN]);
    if (exists.rows.length === 0) {
      const fakeHash = await require('bcryptjs').hash('bot_no_login_' + Date.now(), 6);
      await pool.query(
        'INSERT INTO users (login,password,nickname,banned,muted_until,role,token) VALUES ($1,$2,$3,false,0,$4,$5)',
        [AI_BOT_LOGIN, fakeHash, AI_BOT_NICK, 'bot', require('crypto').randomBytes(32).toString('hex')]
      );
      console.log('[AI Bot] Bot account created');
    }
  } catch(e) { console.log('[AI Bot] init skipped:', e.message); }
}
if (AI_BOT_KEY) initAiBot(pool);

async function askAiBot(userMessage, history) {
  if (!AI_BOT_KEY) return 'AI бот не настроен. Добавьте AI_BOT_KEY в переменные среды.';
  try {
    const messages = (history || []).slice(-10).concat([{ role: 'user', content: userMessage }]);
    const res = await fetch('https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + AI_BOT_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'meta-llama/llama-3.1-8b-instruct:free',
        messages: [{ role: 'system', content: 'Ты дружелюбный помощник в мессенджере MyChat. Отвечай кратко и по делу.' }].concat(messages),
        max_tokens: 500
      })
    });
    const data = await res.json();
    return data.choices?.[0]?.message?.content || 'Не удалось получить ответ';
  } catch(e) {
    console.error('[AI Bot] error:', e);
    return 'Ошибка AI: ' + e.message;
  }
}

// ── SECRET CHATS (auto-delete timer) ─────────────────────
async function initSecretChatTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS secret_chat_settings (
    id SERIAL PRIMARY KEY,
    user_login VARCHAR(50) NOT NULL,
    other_login VARCHAR(50) NOT NULL,
    timer_seconds INT NOT NULL DEFAULT 0,
    UNIQUE(user_login, other_login)
  )`);
}
initSecretChatTables(pool).catch(console.error);

// Auto-delete expired secret messages every 30 seconds
setInterval(async () => {
  try {
    // Delete PM messages where both parties have a timer set
    const settings = await pool.query('SELECT user_login, other_login, timer_seconds FROM secret_chat_settings WHERE timer_seconds > 0');
    for (const row of settings.rows) {
      const cutoff = Date.now() - (row.timer_seconds * 1000);
      const deleted = await pool.query(
        'DELETE FROM private_messages WHERE ((from_login=$1 AND to_login=$2) OR (from_login=$2 AND to_login=$1)) AND timestamp < $3 RETURNING id',
        [row.user_login, row.other_login, cutoff]
      );
      if (deleted.rows.length > 0) {
        const ids = deleted.rows.map(r => r.id);
        const s1 = findSocketByLogin(row.user_login);
        const s2 = findSocketByLogin(row.other_login);
        ids.forEach(id => {
          if (s1) s1.emit('secretMsgDeleted', { id });
          if (s2) s2.emit('secretMsgDeleted', { id });
        });
      }
    }
  } catch(e) {}
}, 30_000);

// ═══════════════════════════════════════════════
// POLLS / ГОЛОСОВАНИЯ
// ═══════════════════════════════════════════════
async function initPollTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS polls (
    id SERIAL PRIMARY KEY,
    chat_type VARCHAR(10) NOT NULL, -- 'general', 'pm', 'room'
    chat_id VARCHAR(100) NOT NULL,  -- login pair or room_id or 'general'
    creator_login VARCHAR(50) NOT NULL,
    question TEXT NOT NULL,
    options JSONB NOT NULL,          -- [{id, text}]
    votes JSONB NOT NULL DEFAULT '{}', -- {login: optionId}
    created_at BIGINT NOT NULL,
    multiple_choice BOOLEAN DEFAULT false,
    anonymous BOOLEAN DEFAULT false
  )`);
}
initPollTables(pool).catch(console.error);


// ═══════════════════════════════════════════════
// PINNED MESSAGES TABLE INIT
// ═══════════════════════════════════════════════
async function initPinnedMsgTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS pinned_messages (
    id SERIAL PRIMARY KEY,
    chat_type VARCHAR(10) NOT NULL,
    chat_id VARCHAR(100) NOT NULL,
    msg_id INT NOT NULL,
    msg_text TEXT,
    msg_user VARCHAR(100),
    pinned_by VARCHAR(50) NOT NULL,
    pinned_at BIGINT NOT NULL,
    UNIQUE(chat_type, chat_id)
  )`);
}
initPinnedMsgTable(pool).catch(console.error);


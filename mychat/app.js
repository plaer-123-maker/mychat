
// ══ TIMING DIAGNOSTICS ══
var _t0 = performance.now();
console.log('[TIMING] app.js start parse:', _t0.toFixed(0), 'ms');
window._perf = { start: _t0 };
/* SOCKET & APP */

var myAuthMethod=null,myGoogleEmail=null;
var myAvatar=null,myUsername=null;
var avatarCache={};
var _tSocket = performance.now();
console.log('[TIMING] socket init:', (_tSocket - window._perf.start).toFixed(0), 'ms after app.js start');
var socket=io({transports:['websocket'],upgrade:false}),isLoginMode=true,myNickname='',myRole='user',myLogin='',
currentView='none',currentPrivateLogin=null,currentRoomId=null,currentRoomData=null,_pmToken=0,_roomToken=0,
_expectedPmLogin=null,_expectedPmToken=-1,_expectedRoomId=null,_expectedRoomToken=-1,
mediaRecorder=null,audioChunks=[],recInterval=null,recSeconds=0,
currentAdminTab='stats',typingTimeout=null,searchTimeout=null,
replyTo=null,myVipUntil=0,myVipEmoji=null,myVerified=false,
editingMsgId=null,editingMsgType=null,
pinnedChats=[],vipEmojiCache={},verifiedCache={},lastSeenCache={},hideOnlineSelf=false,
mutedChats=[];

socket.on('emailChangeCodeSent', function(data) {
  document.getElementById('emailChangeMasked').textContent = data.maskedEmail;
  document.getElementById('emailChangeStep1').style.display='none';
  document.getElementById('emailChangeStep2').style.display='';
  document.getElementById('emailChangeError').textContent='';
});
socket.on('currentEmailVerified', function() {
  document.getElementById('emailChangeStep2').style.display='none';
  document.getElementById('emailChangeStep3').style.display='';
  document.getElementById('emailChangeError').textContent='';
});
socket.on('newEmailCodeSent', function(data) {
  document.getElementById('emailChangeStep3').style.display='none';
  document.getElementById('emailChangeStep4Label').textContent = 'Код отправлен на ' + data.newEmail;
  document.getElementById('emailChangeStep4').style.display='';
  document.getElementById('emailChangeError').textContent='';
});
socket.on('emailChanged', function(data) {
  var s4 = document.getElementById('emailChangeStep4');
  if (s4) s4.style.display='none';
  document.getElementById('emailChangeStepSuccess').style.display='';
  document.getElementById('emailChangeSuccessAddr').textContent = data.newEmail;
  var desc = document.getElementById('settingsEmailDesc');
  if (desc) desc.textContent = data.newEmail;
});
socket.on('emailChangeError', function(msg) {
  document.getElementById('emailChangeError').textContent = msg;
});
socket.on('myEmail', function(data) {
  var el = document.getElementById('settingsEmailDesc');
  if (el) el.textContent = data.email || 'Не привязана';
  if (!data.email && document.getElementById('appContainer').classList.contains('active')) {
    var alreadyShown = localStorage.getItem('attachEmailDismissed_' + myLogin);
    if (!alreadyShown) showAttachEmailModal();
  }
});


/* ── NICK + VIP EMOJI + VERIFIED HELPER ── */
function getNickWithEmoji(login, nickname, inlineEmoji) {
  var emoji = inlineEmoji || (login === myLogin ? myVipEmoji : (vipEmojiCache[login] || null));
  var isVerif = (login === myLogin ? myVerified : (verifiedCache[login] || false));
  var nick = nickname || login;
  var result = nick;
  if (isVerif) result += '<span class="verified-badge" title="Верифицирован">✓</span>';
  if (!emoji) return result;
  var emojiHtml = emoji.startsWith('data:')
    ? '<img src="'+emoji+'" style="width:16px;height:16px;border-radius:50%;object-fit:cover;vertical-align:middle;margin-left:3px;">'
    : '<span class="vip-nick-emoji">'+emoji+'</span>';
  return result + emojiHtml;
}

function getNickPlain(login, nickname) {
  return nickname || login;
}

function formatTime(ts) {
  if (!ts) return '';
  var d = new Date(Number(ts));
  var now = new Date();
  var isToday = d.toDateString() === now.toDateString();
  var h = d.getHours(), m = d.getMinutes();
  var time = (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m;
  if (isToday) return time;
  var yesterday = new Date(now); yesterday.setDate(now.getDate() - 1);
  if (d.toDateString() === yesterday.toDateString()) return 'вчера';
  return d.toLocaleDateString('ru', { day: 'numeric', month: 'short' });
}

function formatLastSeen(ts) {
  if (!ts) return 'давно';
  var diff = Date.now() - ts;
  var min = Math.floor(diff/60000);
  var hr = Math.floor(diff/3600000);
  var days = Math.floor(diff/86400000);
  if (diff < 60000) return 'только что';
  if (min < 60) return min + ' мин назад';
  if (hr < 24) return hr + ' ч назад';
  if (days === 1) return 'вчера';
  return new Date(ts).toLocaleDateString('ru');
}

/* ── NOTIFICATIONS ── */
function initNotifications() {
  if (!('Notification' in window)) return;
  if (Notification.permission === 'default') {
    document.getElementById('notifPermBar').classList.add('show');
  }
}
function requestNotifPermission() {
  Notification.requestPermission().then(function(p) {
    document.getElementById('notifPermBar').classList.remove('show');
    if (p === 'granted') showError('🔔 Уведомления включены!');
  });
}
function dismissNotifBar() {
  document.getElementById('notifPermBar').classList.remove('show');
  localStorage.setItem('notif_dismissed','1');
}
function sendNotification(title, body, icon) {
  if (window.isCapacitorApp) {
    try {
      var LN = window.Capacitor && window.Capacitor.Plugins && window.Capacitor.Plugins.LocalNotifications;
      if (LN) {
        LN.schedule({ notifications: [{
          title: title,
          body: body,
          id: Math.floor(Math.random() * 100000),
          smallIcon: 'ic_notification',
          sound: 'default'
        }]});
        return;
      }
    } catch(e2) {}
  }
  if (Notification.permission !== 'granted') return;
  if (document.hasFocus()) return;
  try { new Notification(title, { body: body, icon: icon || '/favicon.ico' }); } catch(e) {}
}

(function(){var token=localStorage.getItem('mychat_token');if(token)socket.emit('autoLogin',token);})();

/* ── AUTH ── */
var _pendingRegLogin = null;
var _googleSetupData = null;

function showAuthStep(step) {
  ['authStepMain','authStepLogin','authStepRegister','authStepEmailCode','authStepGoogleSetup','authStepTelegramSetup'].forEach(function(id){
    var el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  var map = {'main':'authStepMain','login':'authStepLogin','register':'authStepRegister','emailcode':'authStepEmailCode','logincode':'authStepLoginCode','googlesetup':'authStepGoogleSetup','telegramsetup':'authStepTelegramSetup'};
  if (map[step]) document.getElementById(map[step]).style.display = 'block';
  ['authError','loginError','registerError','emailCodeError','googleSetupError','telegramSetupError'].forEach(function(id){
    var el = document.getElementById(id); if (el) el.textContent = '';
  });
}
showAuthStep('main');

document.getElementById('loginBtn').addEventListener('click', function() {
  var l = document.getElementById('loginLogin').value.trim();
  var p = document.getElementById('loginPassword').value.trim();
  var errEl = document.getElementById('loginError');
  errEl.textContent = '';
  if (!l || !p) { errEl.textContent = 'Заполни все поля'; return; }
  var btn = document.getElementById('loginBtn');
  btn.disabled = true;
  btn.textContent = 'Входим...';
  socket.emit('login', { login: l, password: p });

// Если пользователь нажал Войти пока app.js грузился — выполнить сейчас
if (window._loginPending) {
  window._loginPending = false;
  var _btn = document.getElementById('loginBtn');
  if (_btn) { _btn.disabled = false; _btn.textContent = 'Войти →'; _btn.click(); }
}
  var _restoreBtn = function() { btn.disabled = false; btn.textContent = 'Войти →'; };
  socket.once('authError', _restoreBtn);
  socket.once('authSuccess', _restoreBtn);
});
['loginLogin','loginPassword'].forEach(function(id) {
  var el = document.getElementById(id);
  if (el) el.addEventListener('keypress', function(e) { if (e.key === 'Enter') document.getElementById('loginBtn').click(); });
});

var _pendingLoginLogin = '';
socket.on('loginEmailCodeRequired', function(d) {
  var btn = document.getElementById('loginBtn');
  if (btn) { btn.disabled = false; btn.textContent = 'Войти →'; }
  _pendingLoginLogin = document.getElementById('loginLogin').value.trim();
  document.getElementById('loginCodeHint').textContent = 'Код отправлен на ' + d.email;
  document.getElementById('loginCodeError').textContent = '';
  showAuthStep('logincode');
  setTimeout(function() { var f = document.querySelector('.login-code-digit'); if (f) f.focus(); }, 100);
});

document.querySelectorAll('.login-code-digit').forEach(function(input, i, all) {
  input.addEventListener('input', function() {
    this.value = this.value.replace(/[^0-9]/g, '');
    if (this.value && i < all.length - 1) all[i + 1].focus();
    if (this.value && i === all.length - 1) document.getElementById('confirmLoginCodeBtn').click();
  });
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Backspace' && !this.value && i > 0) all[i - 1].focus();
  });
  input.addEventListener('paste', function(e) {
    e.preventDefault();
    var txt = (e.clipboardData || window.clipboardData).getData('text').replace(/\D/g,'').slice(0,6);
    txt.split('').forEach(function(ch, idx) { if (all[idx]) all[idx].value = ch; });
    if (txt.length === 6) document.getElementById('confirmLoginCodeBtn').click();
  });
});

document.getElementById('confirmLoginCodeBtn').addEventListener('click', function() {
  var code = Array.from(document.querySelectorAll('.login-code-digit')).map(function(i){return i.value;}).join('');
  if (code.length < 6) { document.getElementById('loginCodeError').textContent = 'Введи все 6 цифр'; return; }
  this.disabled = true; this.textContent = 'Проверяем...';
  var btn = this;
  socket.emit('verifyLoginCode', { login: _pendingLoginLogin, code: code });
  socket.once('authError', function(m) { btn.disabled=false; btn.textContent='Подтвердить'; document.getElementById('loginCodeError').textContent = m; });
  socket.once('authSuccess', function() { btn.disabled=false; btn.textContent='Подтвердить'; });
});

document.getElementById('resendLoginCodeLink').addEventListener('click', function() {
  socket.emit('resendLoginCode', { login: _pendingLoginLogin });
  this.textContent = 'Отправлено!';
  var el = this;
  setTimeout(function() { el.textContent = 'Отправить ещё раз'; }, 30000);
});
socket.on('loginCodeResent', function() {
  document.getElementById('loginCodeError').textContent = '';
});

document.getElementById('registerBtn').addEventListener('click', function() {
  var e = document.getElementById('regEmail').value.trim();
  var l = document.getElementById('regLogin').value.trim();
  var n = document.getElementById('regNickname').value.trim();
  var p = document.getElementById('regPassword').value.trim();
  var p2 = document.getElementById('regPassword2').value.trim();
  var err = document.getElementById('registerError');
  err.textContent = '';
  if (!e) { err.textContent = 'Введи email'; return; }
  if (!l) { err.textContent = 'Введи логин'; return; }
  if (!n) { err.textContent = 'Введи отображаемое имя'; return; }
  if (!p) { err.textContent = 'Введи пароль'; return; }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)) { err.textContent = 'Неверный формат email'; return; }
  if (p !== p2) { err.textContent = 'Пароли не совпадают'; return; }
  if (p.length < 6) { err.textContent = 'Пароль минимум 6 символов'; return; }
  var btn = document.getElementById('registerBtn');
  btn.disabled = true;
  btn.textContent = 'Отправляем код...';
  socket.emit('register', { login: l, password: p, nickname: n, email: e });
});

socket.on('emailVerificationRequired', function(d) {
  var btn = document.getElementById('registerBtn');
  if (btn) { btn.disabled = false; btn.textContent = 'Создать аккаунт →'; }
  _pendingRegLogin = d.login;
  document.getElementById('emailCodeHint').textContent = 'Мы отправили 6-значный код на ' + d.email;
  showAuthStep('emailcode');
  setTimeout(function() { var f = document.querySelector('.code-digit'); if (f) f.focus(); }, 100);
});

document.querySelectorAll('.code-digit').forEach(function(input, i, all) {
  input.addEventListener('input', function() {
    this.value = this.value.replace(/[^0-9]/g, '');
    if (this.value && i < all.length - 1) all[i + 1].focus();
    if (this.value && i === all.length - 1) document.getElementById('confirmCodeBtn').click();
  });
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Backspace' && !this.value && i > 0) all[i - 1].focus();
  });
  input.addEventListener('paste', function(e) {
    e.preventDefault();
    var txt = (e.clipboardData || window.clipboardData).getData('text').replace(/\D/g,'').slice(0,6);
    txt.split('').forEach(function(ch, idx) { if (all[idx]) all[idx].value = ch; });
    var last = Math.min(txt.length, all.length - 1);
    if (all[last]) all[last].focus();
  });
});

document.getElementById('confirmCodeBtn').addEventListener('click', function() {
  var code = Array.from(document.querySelectorAll('.code-digit')).map(function(i){ return i.value; }).join('');
  document.getElementById('emailCodeError').textContent = '';
  if (code.length !== 6) { document.getElementById('emailCodeError').textContent = 'Введи все 6 цифр'; return; }
  socket.emit('confirmEmailCode', { login: _pendingRegLogin, code: code });
});

socket.on('emailCodeError', function(msg) {
  document.getElementById('emailCodeError').textContent = msg;
});

document.getElementById('resendCodeLink').addEventListener('click', function() {
  if (!_pendingRegLogin) return;
  socket.emit('resendEmailCode', { login: _pendingRegLogin });
  this.textContent = 'Отправлено!';
  var lnk = this;
  setTimeout(function() { lnk.textContent = 'Отправить код ещё раз'; }, 30000);
});

socket.on('emailCodeResent', function() {
  document.getElementById('emailCodeError').textContent = '';
  document.querySelectorAll('.code-digit').forEach(function(i) { i.value = ''; });
  var f = document.querySelector('.code-digit'); if (f) f.focus();
});

socket.on('googleNeedSetup', function(d) {
  _googleSetupData = d;
  document.getElementById('googleSetupNickname').value = d.suggestedName || '';
  showAuthStep('googlesetup');
});

document.getElementById('googleSetupBtn').addEventListener('click', function() {
  if (!_googleSetupData) return;
  var l = document.getElementById('googleSetupLogin').value.trim();
  var n = document.getElementById('googleSetupNickname').value.trim();
  document.getElementById('googleSetupError').textContent = '';
  if (!l || !n) { document.getElementById('googleSetupError').textContent = 'Заполни все поля'; return; }
  socket.emit('googleRegisterSetup', { googleId: _googleSetupData.googleId, email: _googleSetupData.email, login: l, nickname: n });
});


var _telegramSetupData = null;

socket.on('telegramNeedSetup', function(d) {
  _telegramSetupData = d;
  document.getElementById('telegramSetupLogin').value = (d.tgUsername || '').toLowerCase().replace(/[^a-z0-9_]/g, '').slice(0, 20);
  document.getElementById('telegramSetupNickname').value = d.suggestedName || '';
  showAuthStep('telegramsetup');
});

document.getElementById('telegramSetupBtn').addEventListener('click', function() {
  if (!_telegramSetupData) return;
  var l = document.getElementById('telegramSetupLogin').value.trim();
  var n = document.getElementById('telegramSetupNickname').value.trim();
  document.getElementById('telegramSetupError').textContent = '';
  if (!l || !n) { document.getElementById('telegramSetupError').textContent = 'Заполни все поля'; return; }
  socket.emit('telegramRegisterSetup', { tgId: _telegramSetupData.tgId, login: l, nickname: n });
});

function initTelegramAuth() {
  var botName = window.TELEGRAM_BOT_NAME_PUB;
  if (!botName) return;
  document.getElementById('telegramBtnWrap').style.display = 'block';
  document.getElementById('oauthBtns').style.display = 'block';
  document.getElementById('oauthDivider').style.display = 'flex';

  document.getElementById('telegramSignInBtn').addEventListener('click', function() {
    var w = 550, h = 450;
    var left = (window.screen.width - w) / 2;
    var top = (window.screen.height - h) / 2;
    var authUrl = 'https://oauth.telegram.org/auth?bot_id=' + window.TELEGRAM_BOT_ID_PUB +
      '&origin=' + encodeURIComponent(window.location.origin) +
      '&request_access=write&return_to=' + encodeURIComponent(window.location.origin + '/tg-callback');
    var popup = window.open(authUrl, 'TelegramAuth', 'width=' + w + ',height=' + h + ',left=' + left + ',top=' + top);
    window._tgAuthPopup = popup;
  });
}

window.addEventListener('message', function(e) {
  if (!e.data || e.data.type !== 'telegram_auth') return;
  socket.emit('telegramAuth', e.data.data);
});

window.onTelegramAuth = function(user) {
  socket.emit('telegramAuth', user);
};

window.doGoogleLogin = function() {
  if (!window.GOOGLE_CLIENT_ID_PUB) {
    alert('Вход через Google не настроен. Добавь GOOGLE_CLIENT_ID в Railway Variables.');
    return;
  }
  var nonce = Math.random().toString(36).slice(2);
  var url = 'https://accounts.google.com/o/oauth2/v2/auth'
    + '?client_id=' + encodeURIComponent(window.GOOGLE_CLIENT_ID_PUB)
    + '&redirect_uri=' + encodeURIComponent(window.location.origin)
    + '&response_type=token%20id_token'
    + '&scope=openid%20email%20profile'
    + '&nonce=' + nonce
    + '&prompt=select_account';
  var w=500,h=620,left=Math.round((screen.width-w)/2),top=Math.round((screen.height-h)/2);
  var popup=window.open(url,'googleLogin','width='+w+',height='+h+',left='+left+',top='+top);
  var timer=setInterval(function(){
    try {
      if(!popup||popup.closed){clearInterval(timer);return;}
      var href=popup.location.href;
      if(href&&href.indexOf(window.location.origin)===0){
        clearInterval(timer);
        var hash=popup.location.hash||'';
        popup.close();
        var m=hash.match(/[#&]id_token=([^&]+)/);
        if(m){socket.emit('googleAuth',{idToken:decodeURIComponent(m[1])});}
        else{alert('Не удалось получить токен. Попробуй ещё раз.');}
      }
    }catch(e){}
  },300);
};

function initGoogleAuth(){
  if(typeof google!=='undefined'&&google.accounts&&window.GOOGLE_CLIENT_ID_PUB){
    try{google.accounts.id.initialize({client_id:window.GOOGLE_CLIENT_ID_PUB,use_fedcm_for_prompt:false,callback:function(r){socket.emit('googleAuth',{idToken:r.credential});}});}catch(e){}
  }
}
setTimeout(initGoogleAuth,1000);

socket.on('authSuccess',function(d){
  console.log('[TIMING] authSuccess received:', (performance.now()-window._perf.start).toFixed(0),'ms');
  myNickname=d.nickname;myRole=d.role||'user';myLogin=d.login;
  myAvatar=d.avatar||null;myUsername=d.username||null;
  myVipUntil=d.vip_until||0;myVipEmoji=d.vip_emoji||null;myVerified=d.verified||false;
  myAuthMethod=d.authMethod||null;myGoogleEmail=d.googleEmail||null;
  if(myVipEmoji)vipEmojiCache[myLogin]=myVipEmoji;
  if(d.verified)verifiedCache[d.login]=true;
  if(d.vip_emoji)vipEmojiCache[d.login]=d.vip_emoji;
  if(d.avatar)avatarCache[d.login]=d.avatar;
  if(d.token)localStorage.setItem('mychat_token',d.token);
  document.getElementById('authOverlay').classList.add('hidden');
  document.getElementById('appContainer').classList.add('active');
  if(myRole==='admin'||myRole==='moderator')document.getElementById('adminPanelBtn').style.display='flex';
  if(customBgData) applyBackground('custom', customBgData);
  else if(currentBgId !== 'none') applyBackground(currentBgId, null);
  if (!localStorage.getItem('notif_dismissed')) initNotifications();
  if (window.isCapacitorApp) initCapacitorPush();
  if (window._pushToken && window.socket) window.socket.emit('registerPushToken', { token: window._pushToken, platform: 'android' });
  switchToGeneral();
  console.log('[TIMING] getStartupData emit:', (performance.now()-window._perf.start).toFixed(0),'ms');
  socket.emit('getStartupData');
});
socket.on('authError',function(m){
  localStorage.removeItem('mychat_token');
  var regBtn = document.getElementById('registerBtn');
  if (regBtn && regBtn.disabled) { regBtn.disabled = false; regBtn.textContent = 'Создать аккаунт →'; }
  var shown = false;
  ['loginError','registerError','googleSetupError'].forEach(function(id){
    var el = document.getElementById(id);
    if (el && el.offsetParent !== null && !shown) { el.textContent = m; shown = true; }
  });
  if (!shown) {
    var ae = document.getElementById('authError'); if (ae) ae.textContent = m;
    var le = document.getElementById('loginError'); if (le) le.textContent = m;
  }
});

/* ── READ RECEIPTS ── */
socket.on('messagesRead',function(d){
  d.msgIds.forEach(function(id){
    var el=document.querySelector('#pm-'+id+' .msg-time');
    if(el){ el.innerHTML=el.innerHTML.replace(/<span class="msg-checks[^"]*">.*?<\/span>/,'')+'<span class="msg-checks check-double">✓✓</span>'; }
  });
});

/* ── LAST SEEN / ONLINE STATUS ── */
socket.on('lastSeenResult',function(d){
  lastSeenCache[d.login]=d;
  if(d.isOnline) onlineLogins.add(d.login);
  else onlineLogins.delete(d.login);
  updateHeaderStatus(d.login);
  if(['private','pm','all'].includes(currentView)) renderChatList();
});

function updateHeaderStatus(login){
  if(currentView!=='pm'||currentPrivateLogin!==login)return;
  var onlineEl=document.getElementById('onlineInfo');
  if(!onlineEl)return;
  var d=lastSeenCache[login];
  var isOnline=onlineLogins.has(login);
  if(d&&d.hidden){onlineEl.innerHTML='<span class="status-text offline">был(а) недавно</span>';return;}
  if(isOnline){onlineEl.innerHTML='<span class="status-text">● в сети</span>';return;}
  if(d&&d.last_seen){onlineEl.innerHTML='<span class="status-text offline">был(а) '+formatLastSeen(d.last_seen)+'</span>';return;}
  onlineEl.innerHTML='';
}

/* ── VIP SOCKET HANDLERS ── */
socket.on('vipActivateResult',function(d){
  if(d.ok){
    myVipUntil=d.until;
    var err=document.getElementById('vipCodeError');if(err)err.textContent='';
    openVipSettings();
  } else {
    var err=document.getElementById('vipCodeError');if(err)err.textContent=d.msg||'Ошибка';
  }
});
socket.on('vipEmojiResult',function(d){
  if(d.ok){
    myVipEmoji=d.emoji||null;
    vipEmojiCache[myLogin]=d.emoji||null;
    document.querySelectorAll('.my-nick-display').forEach(function(el){
      el.innerHTML=getNickWithEmoji(myLogin,myNickname);
    });
  }
});

socket.on('userVipUpdated',function(d){
  if(!d.login) return;
  vipEmojiCache[d.login]=d.vip_emoji||null;
  if(d.login===myLogin){
    myVipUntil=d.vip_until||0;
    myVipEmoji=d.vip_emoji||null;
  }
  renderChatList();
});
socket.on('kicked',function(m){localStorage.removeItem('mychat_token');alert(m);location.reload();});
socket.on('nicknameChanged',function(n){myNickname=n;});
socket.on('avatarChanged',function(av){myAvatar=av;if(av)avatarCache[myLogin]=av;else delete avatarCache[myLogin];});
socket.on('usernameChanged',function(un){myUsername=un;});
socket.on('userAvatarUpdated',function(d){if(d.avatar)avatarCache[d.login]=d.avatar;else delete avatarCache[d.login];
  document.querySelectorAll('[data-avatar-login="'+d.login+'"]').forEach(function(el){renderAvatarInEl(el,d.login,el.dataset.avatarNick||'?');});
});

socket.on('avatarOnly', function(d){
  if(!d.login) return;
  if(d.avatar) avatarCache[d.login] = d.avatar;
  var item = document.querySelector('.chat-item[data-chatid="'+d.login+'"]');
  if(item && d.avatar){
    var av = item.querySelector('.avatar');
    if(av && !av.querySelector('img')){
      var online = av.querySelector('.av-online');
      av.style.background='none'; av.style.padding='0';
      av.innerHTML='<img src="'+d.avatar+'" style="width:48px;height:48px;border-radius:50%;object-fit:cover;">'
        +(online?'<div class="av-online" style="display:'+online.style.display+'"></div>':'');
    }
  }
});
socket.on('chatCleared',function(){if(currentView==='general')document.getElementById('messages').innerHTML='';});
socket.on('chatError',function(m){var e=document.getElementById('chatError');e.textContent=m;e.classList.add('show');setTimeout(function(){e.classList.remove('show');},3000);});
function showError(m){var e=document.getElementById('chatError');if(!e)return;e.textContent=m;e.classList.add('show');setTimeout(function(){e.classList.remove('show');},3000);}
window.showError=showError;
socket.on('onlineUsers',function(d){
  if(d.isAdmin) {
    onlineLogins.clear();
    (d.users||[]).forEach(function(u){ onlineLogins.add(u.login); });
    if(['private','pm','all'].includes(currentView)) renderChatList();
  }
  if(currentView==='general'){var el=document.getElementById('onlineInfo');el.textContent=d.isAdmin?(d.count+' онлайн'):(d.count>1?d.count+' онлайн':''); }
});

/* ── TYPING ── */
var typingSet=new Set();
socket.on('userTyping',function(d){if(currentView==='general'){typingSet.add(d.nickname);showTyping();}});
socket.on('userStopTyping',function(d){typingSet.delete(d.nickname);showTyping();});
socket.on('privateUserTyping',function(d){if(currentView==='pm'&&currentPrivateLogin===d.from)document.getElementById('typingIndicator').textContent=d.nickname+' печатает...';});
socket.on('privateUserStopTyping',function(d){if(currentView==='pm'&&currentPrivateLogin===d.from)document.getElementById('typingIndicator').textContent='';});
socket.on('roomUserTyping',function(d){if(currentView==='room'&&currentRoomId==d.roomId){typingSet.add(d.nickname);showTyping();}});
socket.on('roomUserStopTyping',function(d){if(currentView==='room'&&currentRoomId==d.roomId){typingSet.delete(d.nickname);showTyping();}});
function showTyping(){var el=document.getElementById('typingIndicator');if(typingSet.size===0){el.textContent='';return;}var a=Array.from(typingSet);el.textContent=a.join(', ')+(a.length>1?' печатают...':' печатает...');}

/* ── TABS ── */
document.getElementById('tabAll').addEventListener('click',function(){if(currentView==='ghost'){var rid=_ghostRoomId;ghostCleanup();if(rid)socket.emit('ghostLeave',{roomId:rid});}switchToAll();});
document.getElementById('tabGeneral').addEventListener('click',function(){if(currentView==='ghost'){var rid=_ghostRoomId;ghostCleanup();if(rid)socket.emit('ghostLeave',{roomId:rid});}switchToGeneral();});
document.getElementById('tabPrivate').addEventListener('click',function(){if(currentView==='ghost'){var rid=_ghostRoomId;ghostCleanup();if(rid)socket.emit('ghostLeave',{roomId:rid});}switchToPrivate();});
document.getElementById('tabRooms').addEventListener('click',function(){if(currentView==='ghost'){var rid=_ghostRoomId;ghostCleanup();if(rid)socket.emit('ghostLeave',{roomId:rid});}switchToRooms();});
function setActiveTab(id){document.querySelectorAll('.sidebar-tabs button').forEach(function(b){b.classList.remove('active');});document.getElementById(id).classList.add('active');}

function emitGetMyChats(force){
  var now=Date.now();
  if(!force && now-_lastChatsLoad < _CHAT_CACHE_TTL) return;
  _lastChatsLoad=now;
  socket.emit('getMyChats');
}
function emitGetMyRooms(force){
  var now=Date.now();
  if(!force && now-_lastRoomsLoad < _CHAT_CACHE_TTL) return;
  _lastRoomsLoad=now;
  socket.emit('getMyRooms');
}

function switchToAll(){
  _pmToken++; _roomToken++;
  currentView='all';currentPrivateLogin=null;currentRoomId=null;currentRoomData=null;typingSet.clear();
  setActiveTab('tabAll');
  document.getElementById('searchInput').value='';
  if(window.innerWidth<=640)document.getElementById('mainChat').classList.remove('show');
  document.getElementById('noChat').style.display='flex';
  var _msgDivA=document.getElementById('messages');
  _msgDivA.innerHTML='';
  _expectedPmLogin=null;_expectedPmToken=-1;_expectedRoomId=null;_expectedRoomToken=-1;
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='none';
  renderAllChats();
}

function renderAllChats(){
  if(currentView!=='all')return;
  renderChatList();
}

function switchToGeneral(){
  clearReply(); if(typeof cancelEdit === 'function') cancelEdit();
  _pmToken++; _roomToken++;
  currentView='general';currentPrivateLogin=null;currentRoomId=null;currentRoomData=null;typingSet.clear();
  document.getElementById('dialogBgBtn').style.display='none';
  setActiveTab('tabGeneral');
  document.getElementById('searchInput').value='';
  document.getElementById('sidebarList').innerHTML='<div class="chat-item active" onclick="openGeneral()"><div class="avatar">💬</div><div class="info"><div class="name">Общий чат</div><div class="last">Все пользователи</div></div></div>';
  if(window.innerWidth>640){openGeneral();}else{document.getElementById('mainChat').classList.remove('show');document.getElementById('noChat').style.display='';}
}
function switchToPrivate(){
  _roomToken++;
  currentView='private';currentPrivateLogin=null;currentRoomId=null;currentRoomData=null;typingSet.clear();
  setActiveTab('tabPrivate');
  document.getElementById('searchInput').value='';
  if(window.innerWidth<=640)document.getElementById('mainChat').classList.remove('show');
  document.getElementById('noChat').style.display='flex';
  var _msgDiv=document.getElementById('messages');_msgDiv.innerHTML='';
  _expectedPmLogin=null;_expectedPmToken=-1;_expectedRoomId=null;_expectedRoomToken=-1;
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='none';
  if(latestChats&&latestChats.length>0){renderChatList();}else{document.getElementById('sidebarList').innerHTML='<div class="empty-list">Загрузка...</div>';}
  emitGetMyChats(true);
  setTimeout(function(){if(currentView==='private'&&document.getElementById('sidebarList').innerHTML.indexOf('Загрузка')!==-1){document.getElementById('sidebarList').innerHTML='<div class="empty-list">Нет чатов. Найди через поиск ☝️</div>';}},3000);
}
function switchToRooms(){
  _pmToken++; _roomToken++;
  currentView='rooms';currentPrivateLogin=null;currentRoomId=null;currentRoomData=null;typingSet.clear();
  setActiveTab('tabRooms');
  document.getElementById('searchInput').value='';
  if(window.innerWidth<=640)document.getElementById('mainChat').classList.remove('show');
  document.getElementById('noChat').style.display='flex';
  var _msgDivR=document.getElementById('messages');
  _msgDivR.innerHTML='';
  _expectedPmLogin=null;_expectedPmToken=-1;_expectedRoomId=null;_expectedRoomToken=-1;
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='none';
  if(latestRooms&&latestRooms.length>0){renderChatList();}else{document.getElementById('sidebarList').innerHTML='<div class="empty-list">Загрузка...</div>';}
  emitGetMyRooms(true);
}

document.getElementById('searchInput').addEventListener('input',function(){
  var q=this.value.trim();clearTimeout(searchTimeout);
  if(!q){if(currentView==='private')emitGetMyChats();else if(currentView==='rooms')emitGetMyRooms();return;}
  searchTimeout=setTimeout(function(){if(currentView==='private')socket.emit('searchUser',q);else if(currentView==='rooms')socket.emit('searchRooms',q);},300);
});
socket.on('searchResults',function(users){
  if(currentView!=='private')return;var list=document.getElementById('sidebarList');
  if(users.length===0){list.innerHTML='<div class="empty-list">Никого не найдено</div>';return;}
  var html='';users.forEach(function(u){
    if(u.avatar)avatarCache[u.login]=u.avatar;
    var dn=(u.nickname||u.login||'?');
    var avHtml=u.avatar?'<div class="avatar" style="background:none;padding:0;"><img src="'+u.avatar+'" style="width:48px;height:48px;border-radius:50%;object-fit:cover;"></div>':'<div class="avatar">'+dn[0].toUpperCase()+'</div>';
    var subline=u.username?'@'+u.username:'Написать';
    var safeLogin = u.login.replace(/'/g, "\\'");
    var safeNick = dn.replace(/'/g, "\\'");
    html+='<div class="chat-item" onclick="openPM(\''+safeLogin+'\',\''+safeNick+'\')">'+'<div class="avatar" style="background:none;padding:0;">'.replace('>','')+avHtml+'<div class="info"><div class="name">'+dn+'</div><div class="last">'+subline+'</div></div></div>';
  });
  list.innerHTML=html;
});
socket.on('roomSearchResults',function(rooms){
  if(currentView!=='rooms')return;var list=document.getElementById('sidebarList');
  if(rooms.length===0){list.innerHTML='<div class="empty-list">Ничего не найдено</div>';return;}
  var html='';rooms.forEach(function(r){var icon=r.type==='channel'?'📢':'👥';html+='<div class="chat-item" onclick="joinAndOpenRoom('+r.id+')"><div class="avatar">'+icon+'</div><div class="info"><div class="name">'+r.name+'</div><div class="last">Войти</div></div><div class="tag">'+r.type+'</div></div>';});
  list.innerHTML=html;
});

var latestChats=[], latestRooms=[];
var _lastChatsLoad=0, _lastRoomsLoad=0; // timestamps to debounce reloads
var _CHAT_CACHE_TTL = 8000; // ms - don't reload faster than this

function buildChatItem(c, isPinned, chatType) {
  if(c.avatar) avatarCache[c.login||''] = c.avatar;
  var resolvedAvatar = c.avatar || (c.login ? avatarCache[c.login] : null);
  if(c.vip_emoji && c.login) vipEmojiCache[c.login] = c.vip_emoji;
  if(c.verified && c.login) verifiedCache[c.login] = true;
  var lastText='';
  if(c.lastMsg){
    if(c.lastMsg.type==='image') lastText='📷 Фото';
    else if(c.lastMsg.type==='video') lastText='📹 Видео';
    else if(c.lastMsg.type==='file') lastText='📄 Файл: '+(c.lastMsg.text||'');
    else if(c.lastMsg.type==='sticker') lastText='🎭 Стикер '+c.lastMsg.text;
    else if(c.lastMsg.type==='voice') lastText='🎤 Голосовое';
    else if(c.lastMsg.type==='video_note') lastText='⏺ Кружочек';
    else if(c.lastMsg.type==='call') lastText='📞 Звонок';
    else {
      var t = c.lastMsg.text||'';
      if(t.startsWith('{') && t.includes('callType')) { try { var p=JSON.parse(t); lastText=p.answered?'📞 Звонок':'📞 Пропущенный звонок'; } catch(e) { lastText='📞 Звонок'; } }
      else lastText=t;
    }
  }
  var isOnline = c.login && onlineLogins.has(c.login);
  var avHtml;
  if(chatType==='room'){
    avHtml='<div class="avatar">'+(c.type==='channel'?'📢':'👥')+'</div>';
  } else {
    avHtml = resolvedAvatar
      ? '<div class="avatar" style="background:none;padding:0;"><img src="'+resolvedAvatar+'" style="width:48px;height:48px;border-radius:50%;object-fit:cover;"><div class="av-online" style="display:'+(isOnline?'block':'none')+'"></div></div>'
      : '<div class="avatar">'+(c.nickname||c.login||'?')[0].toUpperCase()+'<div class="av-online" style="display:'+(isOnline?'block':'none')+'"></div></div>';
  }
  var nameHtml = getNickWithEmoji(c.login||'', c.name||c.nickname||'');
  var pinClass = isPinned ? ' pinned' : '';
  var pinIcon = isPinned ? '<span class="pin-icon">📌</span>' : '';
  var metaHtml = '';
  if(c.unread>0) metaHtml+='<div class="badge">'+c.unread+'</div>';
  if(c.lastMsg&&c.lastMsg.timestamp) metaHtml='<div class="time">'+formatTime(c.lastMsg.timestamp)+'</div>'+metaHtml;
  var typeBadge = chatType==='room' ? '<span class="chat-type-badge">'+(c.type||'group')+'</span>' : '';
  
  var safeLogin = c.login ? c.login.replace(/'/g, "\\'").replace(/"/g, "&quot;") : '';
  var safeNick = (c.name||c.nickname||'').replace(/'/g, "\\'").replace(/"/g, "&quot;");
  
  var onclickFn = chatType==='room'
    ? 'openRoom('+c.id+')'
    : 'openPM(\''+safeLogin+'\',\''+safeNick+'\')'; 
    
  var isActive = chatType==='room' ? currentRoomId==c.id : currentPrivateLogin===c.login;
  return '<div class="chat-item'+(isActive?' active':'')+pinClass+'" onclick="'+onclickFn+'" oncontextmenu="showMuteMenu(event,\''+chatType+'\','+(chatType==='room'?c.id:'\''+c.login+'\'')+')" data-chattype="'+chatType+'" data-chatid="'+(chatType==='room'?c.id:c.login)+'">'
    + avHtml
    + '<div class="info"><div class="name">'+nameHtml+pinIcon+typeBadge+'</div><div class="last">'+lastText+'</div></div>'
    + (metaHtml?'<div class="meta">'+metaHtml+'</div>':'')
    + '</div>';
}

function renderChatList() {
  if(document.getElementById('searchInput').value.trim()) return;
  var list = document.getElementById('sidebarList');
  if(!list) return;
  if(currentView==='private'||currentView==='pm'||currentView==='rooms'||currentView==='room'||currentView==='all'){
    list.innerHTML='';
  }
  var html = '';
  if(currentView==='private'||currentView==='pm') {
    var pinned = latestChats.filter(function(c){ return isPinned('pm', c.login); });
    var unpinned = latestChats.filter(function(c){ return !isPinned('pm', c.login); });
    if(pinned.length>0){ html+='<div class="pinned-section-title">📌 Закреплённые</div>'; pinned.forEach(function(c){html+=buildChatItem(c,true,'pm');}); }
    if(unpinned.length===0&&pinned.length===0){html='<div class="empty-list">Нет чатов. Найди через поиск ☝️</div>';}
    else unpinned.forEach(function(c){html+=buildChatItem(c,false,'pm');});
    list.innerHTML=html;
  } else if(currentView==='rooms'||currentView==='room') {
    var pinned = latestRooms.filter(function(r){ return isPinned('room', r.id); });
    var unpinned = latestRooms.filter(function(r){ return !isPinned('room', r.id); });
    if(pinned.length>0){ html+='<div class="pinned-section-title">📌 Закреплённые</div>'; pinned.forEach(function(r){html+=buildChatItem(r,true,'room');}); }
    if(unpinned.length===0&&pinned.length===0){html='<div class="empty-list">Нет групп. Создай ➕ или найди через поиск</div>';}
    else unpinned.forEach(function(r){html+=buildChatItem(r,false,'room');});
    list.innerHTML=html;
  } else if(currentView==='all') {
    var allItems=[];
    pinnedChats.forEach(function(p){
      if(p.chat_type==='pm'){var c=latestChats.find(function(x){return x.login===p.chat_id;});if(c)allItems.push({item:c,type:'pm',pinned:true});}
      else{var r=latestRooms.find(function(x){return String(x.id)===String(p.chat_id);});if(r)allItems.push({item:r,type:'room',pinned:true});}
    });
    html += '<div class="chat-item'+(currentView==='general'?' active':'')+'" onclick="openGeneral()"><div class="avatar">💬</div><div class="info"><div class="name">Общий чат</div><div class="last">Все пользователи</div></div></div>';
    if(allItems.length>0){html+='<div class="pinned-section-title">📌 Закреплённые</div>';allItems.forEach(function(a){html+=buildChatItem(a.item,true,a.type);});}
    var pinnedLogins = pinnedChats.filter(function(p){return p.chat_type==='pm';}).map(function(p){return p.chat_id;});
    var pinnedRoomIds = pinnedChats.filter(function(p){return p.chat_type==='room';}).map(function(p){return String(p.chat_id);});
    latestChats.filter(function(c){return !pinnedLogins.includes(c.login);}).forEach(function(c){html+=buildChatItem(c,false,'pm');});
    latestRooms.filter(function(r){return !pinnedRoomIds.includes(String(r.id));}).forEach(function(r){html+=buildChatItem(r,false,'room');});
    list.innerHTML=html;
  }
}

socket.on('myChats',function(chats){
  latestChats=chats;
  window._myChatsCache = chats; // cache for forward modal
  if(currentView==='private'||currentView==='pm'||currentView==='all'){
    if(!document.getElementById('searchInput').value.trim()) renderChatList();
  }
  var missingAvatars = chats.filter(function(c){ return c.login && !avatarCache[c.login]; }).map(function(c){ return c.login; });
  if(missingAvatars.length > 0){
    var batchSize = 5, delay = 0;
    for(var i=0; i<missingAvatars.length; i+=batchSize){
      (function(batch, d){
        setTimeout(function(){
          batch.forEach(function(login){ socket.emit('getAvatarOnly', login); });
        }, d);
      })(missingAvatars.slice(i, i+batchSize), delay);
      delay += 300;
    }
  }
});

socket.on('clearUnreadBadge',function(d){
  var chat = latestChats.find(function(c){return c.login===d.login;});
  if(chat){ chat.unread=0; renderChatList(); }
});
var onlineLogins = new Set();

socket.on('unreadNotification',function(d){
  if(currentView==='pm'&&currentPrivateLogin===d.from)return;
  if(currentView==='private'||currentView==='pm'||currentView==='all')emitGetMyChats();
  if(window.isCapacitorApp){
    sendNotification(d.nickname||'Новое сообщение', d.text||'Написал тебе в MyChat');
  } else if(!document.hasFocus()){
    sendNotification(d.nickname||'Новое сообщение', d.text||'Написал тебе в MyChat');
  }
});

socket.on('myRooms',function(rooms){
  latestRooms=rooms;
  if(currentView!=='rooms'&&currentView!=='room'&&currentView!=='all')return;
  if(document.getElementById('searchInput').value.trim())return;
  renderChatList();
});

var _generalOldestId = null, _generalLoadingMore = false, _generalAllLoaded = false, _generalInitialLoading = false;
var _pmOldestId = null, _pmLoadingMore = false, _pmAllLoaded = false;
var _roomOldestId = null, _roomLoadingMore = false, _roomAllLoaded = false;

function setupScrollPagination(containerEl, loadMoreFn) {
  containerEl.addEventListener('scroll', function() {
    if(containerEl.scrollTop < 80 && !containerEl._loadingMore && !containerEl._allLoaded) {
      loadMoreFn();
    }
  });
}



function openGeneral(){
  if(currentView==='ghost'&&_ghostRoomId){var rid=_ghostRoomId;ghostCleanup();socket.emit('ghostLeave',{roomId:rid});}
  clearReply(); if(typeof cancelEdit === 'function') cancelEdit();
  currentView='general';currentPrivateLogin=null;currentRoomId=null;currentRoomData=null;typingSet.clear();
  _generalOldestId=null;_generalLoadingMore=false;_generalAllLoaded=false;_generalInitialLoading=false;
  document.getElementById('mainChat').classList.add('show');
  document.getElementById('noChat').style.display='none';
  document.getElementById('chatTitle').textContent='Общий чат';
  document.getElementById('chatHeaderAvatar').textContent='💬';
  document.getElementById('typingIndicator').textContent='';
  document.getElementById('onlineInfo').textContent='';
  document.getElementById('chatHeaderBtns').innerHTML='<button onclick="openMsgSearch()" title="Поиск в чате (Ctrl+F)">🔍</button>';
  document.getElementById('inputArea').style.display='flex';
  var _mdg=document.getElementById('messages');
  _mdg.innerHTML='';
  _mdg.dataset.loading='general';
  _mdg._loadingMore=false;_mdg._allLoaded=false;
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='flex';
  _pmToken++; _roomToken++;
  _expectedPmLogin=null;_expectedPmToken=-1;_expectedRoomId=null;_expectedRoomToken=-1;
  _generalInitialLoading=true;
  socket.emit('getGeneralHistory', {});
  _mdg.onscroll = function() {
    if(_generalInitialLoading) return; // ждём пока придут первые 50 сообщений
    if(_mdg.scrollTop < 100 && !_generalLoadingMore && !_generalAllLoaded && _generalOldestId) {
      _generalLoadingMore=true;
      var loader=document.createElement('div');loader.id='load-more-indicator';loader.style.cssText='text-align:center;padding:8px;color:var(--text-muted);font-size:13px;';loader.textContent='Загрузка...';
      _mdg.prepend(loader);
      socket.emit('getGeneralHistory',{before_id:_generalOldestId});
    }
  };
}
window.openGeneral=openGeneral;
function togglePassVis(inputId, btn) {
  var inp = document.getElementById(inputId);
  if (!inp) return;
  if (inp.type === 'password') {
    inp.type = 'text';
    btn.textContent = '🙈';
  } else {
    inp.type = 'password';
    btn.textContent = '👁';
  }
}
window.togglePassVis = togglePassVis;


var currentPMProfile = null;

function updateHeaderAvatar(login, nickname) {
  var ha = document.getElementById('chatHeaderAvatar');
  ha.style.background='';
  ha.style.padding='';
  ha.style.overflow='hidden';
  if(avatarCache[login]) {
    ha.innerHTML='<img src="'+avatarCache[login]+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%;display:block;">';
  } else {
    ha.innerHTML=nickname?nickname[0].toUpperCase():'?';
    ha.style.background='';
  }
}

function onHeaderAvatarClick() {
  if(currentView==='pm'&&currentPrivateLogin) {
    showProfileModal(currentPrivateLogin, currentPMProfile);
  }
}
window.onHeaderAvatarClick=onHeaderAvatarClick;

function showProfileModal(login, profileData) {
  var p = profileData || { login: login, nickname: login, username: null, avatar: avatarCache[login]||null };
  var av = p.avatar || avatarCache[login] || null;
  var nick = p.nickname || login;
  var initial = nick[0].toUpperCase();
  var hdr = document.getElementById('profileModalHeader');
  var bg = document.getElementById('profileModalBgFallback');
  bg.textContent = initial;
  if(av) { bg.style.backgroundImage='url('+av+')'; bg.style.backgroundSize='cover'; bg.style.backgroundPosition='center'; bg.textContent=''; }
  else { bg.style.backgroundImage=''; }
  var avEl = document.getElementById('profileModalAvatar');
  if(av) avEl.innerHTML='<img src="'+av+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">';
  else { avEl.innerHTML='<span>'+initial+'</span>'; avEl.style.fontSize='28px'; }
  document.getElementById('profileModalName').textContent=nick;
  var unEl=document.getElementById('profileModalUsername');
  if(p.username) { unEl.textContent='@'+p.username; unEl.style.display=''; }
  else unEl.style.display='none';
  document.getElementById('profileModalLogin').textContent='логин: '+login;
  var actions=document.getElementById('profileModalActions');
  if(login===myLogin) {
    actions.innerHTML='<button class="pma-msg" onclick="closeProfileModal();openSettings();">⚙️ Настройки</button>';
  } else {
    actions.innerHTML=
      '<button class="pma-msg" onclick="closeProfileModal();openPM(\''+login+'\',\''+nick+'\')" >✉️ Написать</button>'+
      '<button class="pma-call" onclick="closeProfileModal();startCall(\''+login+'\',\'audio\')">📞</button>'+
      '<button class="pma-vcall" onclick="closeProfileModal();startCall(\''+login+'\',\'video\')">📹</button>';
  }
  document.getElementById('profileModalOverlay').classList.add('show');
}
window.showProfileModal=showProfileModal;

function closeProfileModal(){document.getElementById('profileModalOverlay').classList.remove('show');}
window.closeProfileModal=closeProfileModal;

socket.on('userProfile', function(p){
  if(p.avatar) avatarCache[p.login]=p.avatar;
  if(p.verified) verifiedCache[p.login]=true;
  if(p.vip_emoji && p.vip_until > Date.now()) vipEmojiCache[p.login]=p.vip_emoji;
  currentPMProfile=p;
  if(currentView==='pm'&&currentPrivateLogin===p.login) updateHeaderAvatar(p.login, p.nickname);
  if(currentView==='pm'&&currentPrivateLogin===p.login) {
    var titleEl = document.getElementById('chatTitle');
    if(titleEl) titleEl.innerHTML = getNickWithEmoji(p.login, p.nickname);
  }
  var overlay = document.getElementById('profileModalOverlay');
  if (overlay && overlay.classList.contains('show')) {
    var loginEl = document.getElementById('profileModalLogin');
    if (loginEl && loginEl.textContent === 'логин: ' + p.login) {
      showProfileModal(p.login, p);
    }
  }
});

function openPM(login,nickname){
  if(currentView==='ghost'&&_ghostRoomId){var rid=_ghostRoomId;ghostCleanup();socket.emit('ghostLeave',{roomId:rid});}
  clearReply(); if(typeof cancelEdit === 'function') cancelEdit();
  socket.emit('getLastSeen',{login:login});
  currentView='pm';currentPrivateLogin=login;currentPMProfile=null;currentRoomId=null;currentRoomData=null;typingSet.clear();
  setActiveTab('tabPrivate');
  document.getElementById('mainChat').classList.add('show');
  document.getElementById('noChat').style.display='none';
  document.getElementById('chatTitle').textContent=nickname;
  updateHeaderAvatar(login, nickname);
  document.getElementById('typingIndicator').textContent='';
  document.getElementById('onlineInfo').textContent='';
  updatePmHeaderBtns(login, nickname);
  document.getElementById('inputArea').style.display='flex';
  _pmToken++; _roomToken++;
  var _myPmToken=_pmToken;
  _expectedPmLogin=login;
  _expectedPmToken=_myPmToken;
  _expectedRoomId=null;
  _expectedRoomToken=-1;
  var _md=document.getElementById('messages');
  _md.innerHTML='<div class="empty-list">Загрузка...</div>';
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='flex';
  _pmOldestId=null;_pmLoadingMore=false;_pmAllLoaded=false;
  var _md2=document.getElementById('messages');
  _md2._loadingMore=false;_md2._allLoaded=false;
  _md2.onscroll=function(){
    if(_md2.scrollTop<100&&!_pmLoadingMore&&!_pmAllLoaded&&_pmOldestId&&currentView==='pm'){
      _pmLoadingMore=true;_md2._loadingMore=true;
      var loader=document.createElement('div');loader.id='load-more-indicator';loader.style.cssText='text-align:center;padding:8px;color:var(--text-muted);font-size:13px;';loader.textContent='Загрузка...';
      _md2.prepend(loader);
      var tk=_pmToken;socket.emit('getPrivateHistory',{login:currentPrivateLogin,token:tk,before_id:_pmOldestId});
    }
  };
  socket.emit('getPrivateHistory',{login:login,token:_myPmToken});
  socket.emit('getUserProfile',login);
  currentDialogBg = null;
  document.getElementById('dialogBgBtn').style.display='block';
  setTimeout(function() { socket.emit('getDialogBg', { chatType: 'pm', chatId: login }); }, 200);
}
window.openPM=openPM;

function openRoom(roomId){
  if(currentView==='ghost'&&_ghostRoomId){var rid=_ghostRoomId;ghostCleanup();socket.emit('ghostLeave',{roomId:rid});}
  clearReply(); if(typeof cancelEdit === 'function') cancelEdit();
  roomId=Number(roomId);
  currentView='room';currentRoomId=roomId;currentPrivateLogin=null;currentRoomData=null;typingSet.clear();
  setActiveTab('tabRooms');
  document.getElementById('mainChat').classList.add('show');
  document.getElementById('noChat').style.display='none';
  document.getElementById('chatTitle').textContent='Загрузка...';
  document.getElementById('chatHeaderAvatar').textContent='👥';
  document.getElementById('typingIndicator').textContent='';
  document.getElementById('onlineInfo').textContent='';
  document.getElementById('chatHeaderBtns').innerHTML='';
  document.getElementById('messages').innerHTML='<div class="empty-list">Загрузка...</div>';
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='flex';
  _pmToken++; _roomToken++; var _myRoomToken=_roomToken; // invalidate any pending pm responses
  _expectedRoomId=roomId;
  _expectedRoomToken=_myRoomToken;
  _expectedPmLogin=null;
  _expectedPmToken=-1;
  socket.emit('openRoom',{roomId:roomId,token:_myRoomToken});
  currentDialogBg = null;
  document.getElementById('dialogBgBtn').style.display='block';
  setTimeout(function() { socket.emit('getDialogBg', { chatType: 'room', chatId: roomId }); }, 200);
}
window.openRoom=openRoom;

function joinAndOpenRoom(roomId){roomId=Number(roomId);socket.emit('joinRoom',roomId);setTimeout(function(){openRoom(roomId);},500);}
window.joinAndOpenRoom=joinAndOpenRoom;

socket.on('joinedRoom',function(){if(currentView==='rooms'||currentView==='room')emitGetMyRooms();});
socket.on('leftRoom',function(roomId){if(currentRoomId==roomId){currentRoomId=null;currentRoomData=null;currentView='rooms';document.getElementById('mainChat').classList.remove('show');document.getElementById('noChat').style.display='flex';document.getElementById('messages').innerHTML='';}emitGetMyRooms();});
socket.on('kickedFromRoom',function(roomId){if(currentRoomId==roomId){alert('Вас удалили из комнаты');currentRoomId=null;currentRoomData=null;currentView='rooms';document.getElementById('mainChat').classList.remove('show');document.getElementById('noChat').style.display='flex';document.getElementById('messages').innerHTML='';}emitGetMyRooms();});
socket.on('roomDeleted',function(roomId){if(currentRoomId==roomId){alert('Комната удалена');currentRoomId=null;currentRoomData=null;currentView='rooms';document.getElementById('mainChat').classList.remove('show');document.getElementById('noChat').style.display='flex';document.getElementById('messages').innerHTML='';}emitGetMyRooms();});

var _roomOldestId=null,_roomLoadingMore=false,_roomAllLoaded=false;
socket.on('roomData',function(data){
  if(Number(currentRoomId) !== Number(data.room.id)) return;
  var md=document.getElementById('messages');
  var loader=document.getElementById('load-more-indicator');
  if(loader)loader.remove();
  if(_roomLoadingMore){
    _roomLoadingMore=false;
    var msgs=data.messages||[];
    if(!msgs.length){_roomAllLoaded=true;return;}
    var prevH=md.scrollHeight;
    var tmpDiv3=document.createElement('div');
    tmpDiv3.style.display='none';
    document.body.appendChild(tmpDiv3);
    tmpDiv3.id='messages';
    md.id='messages_real';
    msgs.forEach(function(m){addRoomMsg(m);});
    tmpDiv3.id='';
    md.id='messages';
    document.body.removeChild(tmpDiv3);
    var frag3=document.createDocumentFragment();
    while(tmpDiv3.firstChild)frag3.appendChild(tmpDiv3.firstChild);
    md.insertBefore(frag3,md.firstChild);
    md.scrollTop=md.scrollHeight-prevH;
    if(msgs[0])_roomOldestId=msgs[0].id;
    if(data.has_more===false)_roomAllLoaded=true;
    return;
  }
  currentView='room';currentRoomData=data;
  _roomOldestId=null;_roomLoadingMore=false;_roomAllLoaded=false;
  setActiveTab('tabRooms');
  document.getElementById('mainChat').classList.add('show');
  document.getElementById('noChat').style.display='none';
  if(window.innerWidth<=640)document.getElementById('mobileBackBtn').style.display='flex';
  document.getElementById('chatTitle').textContent=data.room.name+(data.room.type==='channel'?' 📢':' 👥');
  document.getElementById('chatHeaderAvatar').textContent=data.room.type==='channel'?'📢':'👥';
  var btns='';
  if(data.myRole==='admin')btns+='<button onclick="openRoomSettings()" title="Настройки">⚙️</button><button onclick="openRoomAnalytics('+data.room.id+')" title="Аналитика">📊</button>';
  btns+='<button onclick="leaveCurrentRoom()" title="Покинуть">🚪</button>';
  if(data.room.type!=='channel'){
    btns+='<button onclick="joinGroupCall(\'audio\')" id="gcHeaderBtn" title="Голосовой звонок" style="font-size:20px;">🔊</button>';
  }
  document.getElementById('chatHeaderBtns').innerHTML=btns+'<button onclick="openSecretTimerModal()" title="Самоудаление сообщений">🔥</button><button onclick="openMsgSearch()" title="Поиск (Ctrl+F)">🔍</button>';
  socket.emit('getGroupCallInfo',{roomId:data.room.id});
  document.getElementById('inputArea').style.display=(data.room.type==='channel'&&data.myRole!=='admin')?'none':'flex';
  md.innerHTML='';
  var msgs=data.messages||[];
  if(!msgs.length){md.innerHTML='<div class="empty-list">Пока нет сообщений</div>';}
  else{msgs.forEach(function(m){addRoomMsg(m);});if(msgs[0])_roomOldestId=msgs[0].id;}
  md.scrollTop=md.scrollHeight;
  if(data.has_more===false)_roomAllLoaded=true;
  md.onscroll=function(){
    if(md.scrollTop<100&&!_roomLoadingMore&&!_roomAllLoaded&&_roomOldestId&&currentView==='room'){
      _roomLoadingMore=true;
      var loader2=document.createElement('div');loader2.id='load-more-indicator';
      loader2.style.cssText='text-align:center;padding:8px;color:var(--text-muted);font-size:13px;';
      loader2.textContent='Загрузка...';md.prepend(loader2);
      socket.emit('openRoom',{roomId:currentRoomId,before_id:_roomOldestId});
    }
  };
  emitGetMyRooms();
});

socket.on('roomNewMessage',function(msg){
  if(currentView==='room'&&currentRoomId==msg.room_id){
    var empty=document.querySelector('#messages .empty-list');if(empty)empty.remove();
    addRoomMsg(msg);document.getElementById('messages').scrollTop=document.getElementById('messages').scrollHeight;
  }
});
socket.on('roomMessageDeleted',function(d){if(currentRoomId==d.roomId){var el=document.getElementById('rmsg-'+d.msgId);if(el){var p=el.parentElement;if(p&&p.classList.contains('msg-wrapper'))p.remove();else el.remove();}}});
socket.on('roomMembersUpdated',function(d){
  if(currentRoomId==d.roomId&&currentRoomData){
    currentRoomData.members=d.members;
    if(document.getElementById('roomSettingsModal').classList.contains('show')) openRoomSettings();
  }
});
socket.on('roomMuted',function(d){
  if(currentRoomId==d.roomId) showToast('Вы замучены в этой группе до '+(d.until?new Date(d.until).toLocaleTimeString():'снято'));
});
socket.on('roomBanned',function(d){
  if(currentRoomId==d.roomId){
    showToast('Вас забанили в группе');
    setTimeout(function(){socket.emit('leaveRoom',d.roomId);switchToGeneral();},1500);
  }
});
socket.on('roomSettingsUpdated',function(d){if(currentRoomId==d.roomId&&currentRoomData)currentRoomData.room.comments_enabled=d.comments_enabled;});

function addRoomMsg(msg){
  var div=document.createElement('div');var isMine=msg.user_login===myLogin;
  div.className='message '+(isMine?'mine':'other');div.id='rmsg-'+msg.id;div.setAttribute('data-msg-id',msg.id||'');
  if(msg.user_login&&msg.vip_emoji)vipEmojiCache[msg.user_login]=msg.vip_emoji;
  var html='<div class="name msg-nick" data-login="'+escHtml(msg.user_login||'')+'" onclick="onMsgNickClick(this)">'+getNickWithEmoji(msg.user_login||'',msg.username,msg.vip_emoji)+'</div>';
  html+=buildReplyQuote(msg);
  if(msg.type==='image'&&msg.image){
    var imgs=(function(){var p=parseImgField(msg.image);return Array.isArray(p)?p:[p];})();
    var msgId=msg.id||msg._id||'';
    if(imgs.length===1){
      html+='<div class="photo-img-wrap" data-msgs="'+encodeURIComponent(JSON.stringify(imgs))+'" data-msgid="'+msgId+'" data-idx="0" onclick="pvClickHandler(this,event)"><img src="'+imgs[0]+'" class="chat-photo" alt="фото"></div>';
    } else {
      var cnt=Math.min(imgs.length,4);
      html+='<div class="photo-grid count-'+cnt+'" data-msgs="'+encodeURIComponent(JSON.stringify(imgs))+'" data-msgid="'+msgId+'">';
      for(var pi=0;pi<cnt;pi++){html+='<img src="'+imgs[pi]+'" class="chat-photo" alt="фото" data-idx="'+pi+'" onclick="pvClickGridHandler(this.parentNode,pi,event)">'; }
      html+='</div>';
    }
  }
  else if(msg.type==='video_note'&&msg.voice){}
  else if(msg.type==='voice'&&msg.voice)html+='<audio controls src="'+msg.voice+'"></audio>';
  else if(msg.type==='sticker')html+='<span style="font-size:52px;line-height:1.2;display:block;">'+escHtml(msg.text||'')+'</span>';
  else if(msg.type==='photo_sticker'&&msg.image)html+='<img src="'+msg.image+'" style="max-width:160px;max-height:160px;border-radius:18px;display:block;margin:2px 0;cursor:pointer;" onclick="window.pvSingle&&pvSingle(msg.image)">';
  else if(msg.type==='video'&&(msg.file_url||msg.image))html+='<video src="'+(msg.file_url||msg.image)+'" controls style="max-width:260px;max-height:200px;border-radius:10px;display:block;margin-top:4px;"></video>'+(msg.file_name?'<div style="font-size:11px;color:var(--text-muted);margin-top:2px;">📹 '+escHtml(msg.file_name)+'</div>':'');
  else if(msg.type==='file'&&msg.file_url){var fsz=msg.file_size?' ('+Math.round(msg.file_size/1024)+' KB)':'';html+='<a href="'+msg.file_url+'" download="'+(msg.file_name||'file')+'" style="display:flex;align-items:center;gap:8px;background:var(--bg-input);padding:10px 14px;border-radius:10px;color:var(--text-primary);text-decoration:none;margin-top:4px;max-width:260px;"><span style="font-size:24px;">📄</span><div><div style="font-size:13px;font-weight:600;">'+escHtml(msg.file_name||'Файл')+'</div><div style="font-size:11px;color:var(--text-muted);">'+fsz+'</div></div></a>';}
  else if(msg.type==='location'&&msg.text){var geoHtml=buildLocationHtml(msg.text);html+=geoHtml||'<div class="text">'+formatText(msg.text||'')+'</div>';}
  else html+='<div class="text">'+formatText(msg.text||'')+'</div>';
  if(msg.fwd_from_nick)html='<div class="fwd-header">📤 Переслано от: '+escHtml(msg.fwd_from_nick)+'</div>'+html.replace('<div class="name','<div class="name');
  html+=buildMsgTime(msg.timestamp);
  if(isMine&&msg.id)html=html.replace('</div>','<span class="msg-checks check-single" style="cursor:pointer;margin-left:4px;" onclick="showReadersPopup(event,'+msg.id+')" title="Кто прочитал">✓</span></div>');
  if(isMine||(currentRoomData&&currentRoomData.myRole==='admin'))html+='<span class="del-btn" onclick="delRoomMsg('+msg.id+')">Удалить</span>';
  div.innerHTML=html;
  if(msg.type==='video_note'&&msg.voice){div.classList.add('video-note-msg');div.appendChild(renderVideoNote(msg.voice,isMine,msg.id));}
  attachMsgEvents(div,msg.id,'room',msg.username,msg.text||'');
  document.getElementById('messages').appendChild(div._wrapper||div);
}
function delRoomMsg(id){socket.emit('deleteRoomMessage',{roomId:currentRoomId,msgId:id});}
window.delRoomMsg=delRoomMsg;
function leaveCurrentRoom(){if(currentRoomId&&confirm('Покинуть комнату?'))socket.emit('leaveRoom',currentRoomId);}
window.leaveCurrentRoom=leaveCurrentRoom;

function openRoomSettings(){
  if(!currentRoomData)return;var d=currentRoomData;
  var html='<h3>⚙️ '+d.room.name+'</h3>';
  if(d.room.type==='channel')html+='<div style="margin-bottom:15px;"><button class="save" style="width:100%;padding:10px;border:none;border-radius:8px;background:'+(d.room.comments_enabled?'#e94560':'#27ae60')+';color:#fff;cursor:pointer;" onclick="toggleComments()">Комментарии: '+(d.room.comments_enabled?'ВКЛ — выключить':'ВЫКЛ — включить')+'</button></div>';
  html+='<div class="members-panel"><h4>Участники ('+d.members.length+')</h4>';
  html+='<div style="margin-bottom:8px;display:flex;gap:4px;"><input id="addMemberInput" placeholder="Логин" style="flex:1;padding:8px;border:1.5px solid var(--border);border-radius:8px;background:var(--bg-input);color:var(--text-primary);outline:none;"><button style="padding:8px 12px;border:none;border-radius:8px;background:#27ae60;color:#fff;cursor:pointer;font-weight:600;" onclick="addMember()">+</button></div>';
  d.members.forEach(function(m){
    var isMuted = m.muted_until && m.muted_until > Date.now();
    var isBanned = m.banned;
    var statusBadge = isBanned ? ' <span style="color:#e94560;font-size:11px;">🚫забанен</span>'
                    : isMuted  ? ' <span style="color:#f39c12;font-size:11px;">🔇замучен</span>' : '';
    html+='<div class="member-row"><span>'+m.nickname+' ('+m.user_login+')'+statusBadge+' <span class="role '+m.role+'">'+m.role+'</span></span>';
    if(m.user_login!==d.room.owner_login && d.myRole==='admin'){
      html+='<div class="mr-btns">';
      if(m.role==='member') html+='<button style="background:#9b59b6;" onclick="setRoomRole(\''+m.user_login+'\',\'admin\')">→Админ</button>';
      else html+='<button style="background:var(--bg-base);" onclick="setRoomRole(\''+m.user_login+'\',\'member\')">→Участник</button>';
      if(isMuted) html+='<button style="background:#f39c12;" onclick="roomUnmuteMember(\''+m.user_login+'\')">Размут</button>';
      else html+='<button style="background:#e67e22;" onclick="roomMuteMemberPrompt(\''+m.user_login+'\')">🔇Мут</button>';
      if(isBanned) html+='<button style="background:#27ae60;" onclick="roomUnbanMember(\''+m.user_login+'\')">Разбан</button>';
      else html+='<button style="background:#c0392b;" onclick="roomBanMember(\''+m.user_login+'\')">🚫Бан</button>';
      html+='<button style="background:#7f8c8d;" onclick="removeMember(\''+m.user_login+'\')">✕Кик</button>';
      html+='</div>';
    }
    html+='</div>';
  });
  html+='</div>';
  if(d.room.owner_login===myLogin)html+='<button style="width:100%;padding:10px;border:none;border-radius:8px;background:#c0392b;color:#fff;cursor:pointer;margin-top:10px;font-weight:600;" onclick="deleteCurrentRoom()">🗑 Удалить комнату</button>';
  html+='<button style="width:100%;padding:10px;border:none;border-radius:8px;background:var(--bg-input);color:var(--text-secondary);cursor:pointer;margin-top:8px;" onclick="closeAllModals()">Закрыть</button>';
  document.getElementById('roomSettingsContent').innerHTML=html;
  document.getElementById('roomSettingsModal').classList.add('show');
}
window.openRoomSettings=openRoomSettings;
function addMember(){var l=document.getElementById('addMemberInput').value.trim();if(l){socket.emit('roomAddMember',{roomId:currentRoomId,login:l});document.getElementById('addMemberInput').value='';}}
window.addMember=addMember;
function removeMember(login){if(confirm('Удалить '+login+'?'))socket.emit('roomRemoveMember',{roomId:currentRoomId,login:login});}
window.removeMember=removeMember;
function setRoomRole(login,role){socket.emit('roomSetRole',{roomId:currentRoomId,login:login,role:role});}
window.setRoomRole=setRoomRole;
function roomMuteMemberPrompt(login){
  var min=prompt('Замутить '+login+' на сколько минут? (0 = снять мут)','30');
  if(min===null) return;
  socket.emit('roomMuteMember',{roomId:currentRoomId,login:login,minutes:parseInt(min)||0});
}
window.roomMuteMemberPrompt=roomMuteMemberPrompt;
function roomUnmuteMember(login){socket.emit('roomMuteMember',{roomId:currentRoomId,login:login,minutes:0});}
window.roomUnmuteMember=roomUnmuteMember;
function roomBanMember(login){if(confirm('Забанить '+login+' в этой группе?'))socket.emit('roomBanMember',{roomId:currentRoomId,login:login,banned:true});}
window.roomBanMember=roomBanMember;
function roomUnbanMember(login){socket.emit('roomBanMember',{roomId:currentRoomId,login:login,banned:false});}
window.roomUnbanMember=roomUnbanMember;
window.setRoomRole=setRoomRole;
function toggleComments(){socket.emit('roomToggleComments',currentRoomId);}
window.toggleComments=toggleComments;
function deleteCurrentRoom(){if(confirm('Удалить комнату? Необратимо!'))socket.emit('deleteRoom',currentRoomId);}
window.deleteCurrentRoom=deleteCurrentRoom;
function closeAllModals(){document.querySelectorAll('.modal-overlay').forEach(function(m){m.classList.remove('show');});}
window.closeAllModals=closeAllModals;

/* ── PRIVATE MESSAGES ── */
socket.on('privateHistory',function(d){
  if(currentView !== 'pm') return;
  if(currentPrivateLogin !== d.otherLogin) return;
  var msgs = d.messages || [];
  var hasMore = d.has_more !== false && msgs.length >= 50;
  var md=document.getElementById('messages');
  var loader=document.getElementById('load-more-indicator');
  if(loader)loader.remove();
  if(_pmLoadingMore){
    _pmLoadingMore=false;
    if(!msgs||msgs.length===0){_pmAllLoaded=true;return;}
    var prevH=md.scrollHeight;
    var tmpDiv2=document.createElement('div');
    tmpDiv2.style.display='none';
    document.body.appendChild(tmpDiv2);
    tmpDiv2.id='messages';
    md.id='messages_real';
    msgs.forEach(function(m){addPM(m);});
    tmpDiv2.id='';
    md.id='messages';
    document.body.removeChild(tmpDiv2);
    var frag2=document.createDocumentFragment();
    while(tmpDiv2.firstChild)frag2.appendChild(tmpDiv2.firstChild);
    md.insertBefore(frag2,md.firstChild);
    md.scrollTop=md.scrollHeight-prevH;
    if(msgs[0])_pmOldestId=msgs[0].id;
    if(!hasMore)_pmAllLoaded=true;
    return;
  }
  md.innerHTML='';
  if(!msgs||msgs.length===0){md.innerHTML='<div class="empty-list">Начните диалог 👋</div>';return;}
  msgs.forEach(function(m){addPM(m);});
  md.scrollTop=md.scrollHeight;
  if(msgs[0])_pmOldestId=msgs[0].id;
  _pmAllLoaded=!hasMore;md._allLoaded=!hasMore;
});
socket.on('rateLimited',function(d){
  var msg = (d && d.msg) ? d.msg : 'Слишком много запросов. Подождите.';
  var toast = document.getElementById('_rateLimitToast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = '_rateLimitToast';
    toast.style.cssText = 'position:fixed;bottom:80px;left:50%;transform:translateX(-50%);background:#e94560;color:#fff;padding:10px 20px;border-radius:10px;font-size:14px;font-weight:600;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,0.4);transition:opacity 0.3s;';
    document.body.appendChild(toast);
  }
  toast.textContent = msg;
  toast.style.opacity = '1';
  clearTimeout(toast._hideTimeout);
  toast._hideTimeout = setTimeout(function(){ toast.style.opacity = '0'; }, 3000);
});

socket.on('newPrivateMessage',function(msg){
  if(msg.from_login==='_ai_bot'){handleAiBotMessage(msg);return;}
  if(currentView==='pm'&&(currentPrivateLogin===msg.from_login||currentPrivateLogin===msg.to_login)){
    if(msg.id && document.getElementById('pm-'+msg.id)) return;
    var empty=document.querySelector('#messages .empty-list');if(empty)empty.remove();
    addPM(msg);document.getElementById('messages').scrollTop=document.getElementById('messages').scrollHeight;
  }
  if(currentView==='private'||currentView==='pm')emitGetMyChats();
});
socket.on('privateMessageDeleted',function(d){var el=document.getElementById('pm-'+d.id);if(el){var p=el.parentElement;if(p&&p.classList.contains('msg-wrapper'))p.remove();else el.remove();}});

function buildCallLogHtml(msg) {
  try {
    var d=JSON.parse(msg.text||'{}');
    var isOutgoing=d.callerLogin===myLogin;
    var callTypeLabel=d.callType==='video'?'видеозвонок':'аудиозвонок';
    var icon, title, sub='', iconColor;

    var timeStr=formatTime(msg.timestamp);

    if(d.missed || !d.answered) {
      if(isOutgoing) {
        icon='📵'; iconColor='#ff6b6b';
        title='Нет ответа';
      } else {
        icon='📵'; iconColor='#ff6b6b';
        title='Пропущенный '+callTypeLabel;
      }
      sub=timeStr;
    } else {
      icon=d.callType==='video'?'📹':'📞';
      iconColor=isOutgoing?'var(--accent)':'var(--online)';
      title=(isOutgoing?'Исходящий ':'Входящий ')+callTypeLabel;
      if(d.duration>0){
        var m=Math.floor(d.duration/60),s=d.duration%60;
        sub=(m>0?m+' мин ':'')+s+' с';
      } else {
        sub=timeStr;
      }
    }

    var otherLogin=isOutgoing?d.calleeLogin:d.callerLogin;
    return '<div class="call-log-msg" onclick="startCall(\''+otherLogin+'\',\''+d.callType+'\')">'
      +'<span class="cl-icon" style="color:'+iconColor+'">'+icon+'</span>'
      +'<div class="cl-info">'
        +'<div class="cl-title">'+title+'</div>'
        +(sub?'<div class="cl-sub">'+sub+'</div>':'')
      +'</div>'
      +'<span style="color:var(--accent);font-size:13px;font-weight:600;">↩</span>'
      +'</div>';
  } catch(e){ return '<div class="call-log-msg"><span class="cl-icon">📞</span><div class="cl-info"><div class="cl-title">Звонок</div></div></div>'; }
}

function addPM(msg){
  if(msg.type==='call_log') {
    var wrapper=document.createElement('div');
    wrapper.style.cssText='display:flex;justify-content:center;padding:6px 0;';
    wrapper.id='pm-'+msg.id;
    wrapper.innerHTML=buildCallLogHtml(msg)+'<div style="position:absolute;right:0;bottom:0;"></div>';
    wrapper.style.position='relative';
    var timeEl=document.createElement('div');
    timeEl.style.cssText='text-align:center;font-size:11px;color:var(--text-muted);margin-bottom:4px;';
    var d=new Date(Number(msg.timestamp));var h=d.getHours(),m=d.getMinutes();
    timeEl.textContent=(h<10?'0':'')+h+':'+(m<10?'0':'')+m;
    var container=document.createElement('div');
    container.style.cssText='display:flex;flex-direction:column;align-items:center;gap:2px;';
    container.appendChild(wrapper);
    container.appendChild(timeEl);
    document.getElementById('messages').appendChild(container);
    return;
  }
  var div=document.createElement('div');var isMine=msg.from_login===myLogin;
  div.className='message '+(isMine?'mine':'other');div.id='pm-'+msg.id;div.setAttribute('data-msg-id',msg.id||'');
  var html='';
  if(msg.from_login&&msg.vip_emoji)vipEmojiCache[msg.from_login]=msg.vip_emoji;
  if(!isMine)html+='<div class="name msg-nick" data-login="'+escHtml(msg.from_login||'')+'" onclick="onMsgNickClick(this)">'+getNickWithEmoji(msg.from_login||'',msg.from_nickname,msg.vip_emoji)+'</div>';
  html+=buildReplyQuote(msg);
  if(msg.type==='image'&&msg.image){
    var imgs=(function(){var p=parseImgField(msg.image);return Array.isArray(p)?p:[p];})();
    var msgId=msg.id||msg._id||'';
    if(imgs.length===1){
      html+='<div class="photo-img-wrap" data-msgs="'+encodeURIComponent(JSON.stringify(imgs))+'" data-msgid="'+msgId+'" data-idx="0" onclick="pvClickHandler(this,event)"><img src="'+imgs[0]+'" class="chat-photo" alt="фото"></div>';
    } else {
      var cnt=Math.min(imgs.length,4);
      html+='<div class="photo-grid count-'+cnt+'" data-msgs="'+encodeURIComponent(JSON.stringify(imgs))+'" data-msgid="'+msgId+'">';
      for(var pi=0;pi<cnt;pi++){html+='<img src="'+imgs[pi]+'" class="chat-photo" alt="фото" data-idx="'+pi+'" onclick="pvClickGridHandler(this.parentNode,pi,event)">'; }
      html+='</div>';
    }
  }
  else if(msg.type==='video_note'&&msg.voice){}
  else if(msg.type==='voice'&&msg.voice)html+='<audio controls src="'+msg.voice+'"></audio>';
  else if(msg.type==='sticker')html+='<span style="font-size:52px;line-height:1.2;display:block;">'+escHtml(msg.text||'')+'</span>';
  else if(msg.type==='photo_sticker'&&msg.image)html+='<img src="'+msg.image+'" style="max-width:160px;max-height:160px;border-radius:18px;display:block;margin:2px 0;cursor:pointer;" onclick="window.pvSingle&&pvSingle(msg.image)">';
  else if(msg.type==='video'&&(msg.file_url||msg.image))html+='<video src="'+(msg.file_url||msg.image)+'" controls style="max-width:260px;max-height:200px;border-radius:10px;display:block;margin-top:4px;"></video>'+(msg.file_name?'<div style="font-size:11px;color:var(--text-muted);margin-top:2px;">📹 '+escHtml(msg.file_name)+'</div>':'');
  else if(msg.type==='file'&&msg.file_url){var fsz2=msg.file_size?' ('+Math.round(msg.file_size/1024)+' KB)':'';html+='<a href="'+msg.file_url+'" download="'+(msg.file_name||'file')+'" style="display:flex;align-items:center;gap:8px;background:var(--bg-input);padding:10px 14px;border-radius:10px;color:var(--text-primary);text-decoration:none;margin-top:4px;max-width:260px;"><span style="font-size:24px;">📄</span><div><div style="font-size:13px;font-weight:600;">'+escHtml(msg.file_name||'Файл')+'</div><div style="font-size:11px;color:var(--text-muted);">'+fsz2+'</div></div></a>';}
  else if(msg.type==='location'&&msg.text){var geoHtml=buildLocationHtml(msg.text);html+=geoHtml||'<div class="text">'+formatText(msg.text||'')+'</div>';}
  else html+='<div class="text">'+formatText(msg.text||'')+'</div>';
  if(msg.fwd_from_nick)html='<div class="fwd-header">📤 Переслано от: '+escHtml(msg.fwd_from_nick)+'</div>'+html;
  html+=buildMsgTime(msg.timestamp);
  if(isMine||myRole==='admin')html+='<span class="del-btn" onclick="delPM('+msg.id+')">Удалить</span>';
  div.innerHTML=html;
  if(msg.type==='video_note'&&msg.voice){div.classList.add('video-note-msg');div.appendChild(renderVideoNote(msg.voice,isMine,msg.id));}
  attachMsgEvents(div,msg.id,'pm',msg.from_nickname,msg.text||'');
  document.getElementById('messages').appendChild(div._wrapper||div);
}
function delPM(id){socket.emit('deletePrivateMessage',id);}
window.delPM=delPM;

/* ── GENERAL CHAT ── */
socket.on('messageHistory',function(data){
  console.log('[TIMING] messageHistory received:', (performance.now()-window._perf.start).toFixed(0),'ms — CHAT LOADED');
  var msgs = Array.isArray(data) ? data : (data.msgs || data);
  var hasMore = Array.isArray(data) ? (msgs.length>=50) : (data.has_more !== false);
  if(currentView!=='general')return;
  var md=document.getElementById('messages');
  var loader=document.getElementById('load-more-indicator');
  if(loader)loader.remove();
  if(_generalLoadingMore){
    _generalLoadingMore=false;
    if(!msgs||msgs.length===0){_generalAllLoaded=true;return;}
    var _existingIds={};
    md.querySelectorAll('[id^="msg-"]').forEach(function(el){_existingIds[el.id]=1;});
    var newMsgs=msgs.filter(function(m){return !_existingIds['msg-'+m.id];});
    if(!newMsgs.length){if(!hasMore)_generalAllLoaded=true;return;}
    var prevScrollTop=md.scrollTop;
    var prevH=md.scrollHeight;
    md.id='messages_bak';
    var tmpDiv=document.createElement('div');
    tmpDiv.id='messages';
    tmpDiv.style.cssText='position:fixed;left:-99999px;top:0;visibility:hidden;pointer-events:none;';
    document.body.appendChild(tmpDiv);
    newMsgs.forEach(function(m){addMsg(m);});
    tmpDiv.id='';
    md.id='messages';
    document.body.removeChild(tmpDiv);
    var frag=document.createDocumentFragment();
    while(tmpDiv.firstChild)frag.appendChild(tmpDiv.firstChild);
    md.insertBefore(frag,md.firstChild);
    md.scrollTop=prevScrollTop+(md.scrollHeight-prevH);
    if(msgs[0])_generalOldestId=msgs[0].id;
    if(!hasMore)_generalAllLoaded=true;
    return;
  }
  if(md.dataset.loading!=='general')return;
  delete md.dataset.loading; // снимаем флаг сразу чтобы не попасть сюда повторно
  md.innerHTML='';
  if(!msgs||msgs.length===0){
    _generalInitialLoading=false;
    md.innerHTML='<div class="empty-list">Пока нет сообщений</div>';return;
  }
  msgs.forEach(function(m){addMsg(m);});
  md.scrollTop=md.scrollHeight;
  if(msgs[0])_generalOldestId=msgs[0].id;
  _generalAllLoaded=!hasMore;
  _generalInitialLoading=false; // разблокируем скролл-пагинацию
});
socket.on('chatMessage',function(msg){
  if(currentView!=='general')return;
  var empty=document.querySelector('#messages .empty-list');if(empty)empty.remove();
  var _md=document.getElementById('messages');
  var _atBottom=(_md.scrollHeight-_md.scrollTop-_md.clientHeight)<150;
  addMsg(msg);
  if(_atBottom) _md.scrollTop=_md.scrollHeight;
  setTimeout(observeAllLazy,100);
});
socket.on('messageDeleted',function(id){var el=document.getElementById('msg-'+id);if(el){var p=el.parentElement;if(p&&p.classList.contains('msg-wrapper'))p.remove();else el.remove();}});

/* ── AVATAR HELPERS ── */
function getAvatarHtml(login, nickname, size) {
  size=size||48;
  var av=avatarCache[login];
  if(av)return'<div class="avatar" style="width:'+size+'px;height:'+size+'px;background:none;padding:0;" data-avatar-login="'+login+'" data-avatar-nick="'+escHtml(nickname||'?')+'"><img src="'+av+'" style="width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;"></div>';
  return'<div class="avatar" style="width:'+size+'px;height:'+size+'px;" data-avatar-login="'+login+'" data-avatar-nick="'+escHtml(nickname||'?')+'">'+(nickname?nickname[0].toUpperCase():'?')+'</div>';
}
function renderAvatarInEl(el, login, nickname) {
  var av=avatarCache[login];
  if(av){el.style.background='none';el.style.padding='0';el.innerHTML='<img src="'+av+'" style="width:100%;height:100%;border-radius:50%;object-fit:cover;display:block;">';}
  else{el.style.background='';el.style.padding='';el.innerHTML=(nickname?nickname[0].toUpperCase():'?');}
}
function escHtml(s){return s?s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'):''}

function formatText(s){
  if(!s)return'';
  var t=escHtml(s);
  t=t.replace(/```([\s\S]*?)```/g,'<pre class="msg-code-block">$1</pre>');
  t=t.replace(/`([^`\n]+)`/g,'<code class="msg-code">$1</code>');
  // Bold **text** or __text__
  t=t.replace(/\*\*([^*\n]+)\*\*/g,'<b>$1</b>');
  t=t.replace(/__([^_\n]+)__/g,'<b>$1</b>');
  // Italic *text* or _text_
  t=t.replace(/\*([^*\n]+)\*/g,'<i>$1</i>');
  t=t.replace(/_([^_\n]+)_/g,'<i>$1</i>');
  // Strikethrough ~~text~~
  t=t.replace(/~~([^~\n]+)~~/g,'<s>$1</s>');
  // Spoiler ||text||
  t=t.replace(/\|\|([^|\n]+)\|\|/g,'<span class="msg-spoiler" onclick="this.classList.toggle(\'revealed\')">$1</span>');
  // URLs
  t=t.replace(/(https?:\/\/[^\s<>"]+)/g,'<a href="$1" target="_blank" rel="noopener noreferrer" class="msg-link">$1</a>');
  // Newlines
  t=t.replace(/\n/g,'<br>');
  return t;
}

// Client-side image parsing: handles both string URLs and JSON arrays from DB
function parseImgField(img){
  if(!img)return null;
  if(Array.isArray(img))return img;
  if(typeof img==='string'&&img.startsWith('[')){
    try{return JSON.parse(img);}catch(e){}
  }
  return img;
}

function buildMsgTime(ts) {
  if(!ts)return '';
  var d=new Date(Number(ts));
  var h=d.getHours(),m=d.getMinutes();
  return '<div class="msg-time">'+(h<10?'0':'')+h+':'+(m<10?'0':'')+m+'</div>';
}

/* ── REPLY ── */
document.getElementById('replyCancel').addEventListener('click',clearReply);
function clearReply(){replyTo=null;document.getElementById('replyBar').classList.remove('active');document.getElementById('replyBarName').textContent='';document.getElementById('replyBarText').textContent='';}
function setReply(id,user,text){replyTo={id:id,user:user,text:text};document.getElementById('replyBar').classList.add('active');document.getElementById('replyBarName').textContent='↩ '+user;document.getElementById('replyBarText').textContent=text||'📎 Медиа';document.getElementById('msgInput').focus();}
window.setReply=setReply;

/* ── REACTION PANEL ── */
var EMOJIS=['❤️','👍','😂','🔥','😮','😢','👏','🎉'];
var activePanel=null;
function closeReactionPanel(){if(activePanel){activePanel.remove();activePanel=null;}}
function showReactionPanel(msgId,msgType,anchorEl,replyUser,replyText,canDelete){
  closeReactionPanel();
  var panel=document.createElement('div');panel.className='reaction-panel';
  var rb=document.createElement('button');rb.className='rp-reply';rb.textContent='↩ Ответить';
  rb.addEventListener('click',function(e){e.stopPropagation();setReply(msgId,replyUser,replyText);closeReactionPanel();});
  panel.appendChild(rb);
  var div1=document.createElement('div');div1.className='rp-divider';panel.appendChild(div1);
  EMOJIS.forEach(function(em){
    var b=document.createElement('button');b.textContent=em;
    b.addEventListener('click',function(e){e.stopPropagation();socket.emit('addReaction',{msgType:msgType,msgId:msgId,emoji:em});closeReactionPanel();});
    panel.appendChild(b);
  });
  if(canDelete){
    var div2=document.createElement('div');div2.className='rp-divider';panel.appendChild(div2);
    var db=document.createElement('button');db.className='rp-delete';db.textContent='🗑 Удалить';
    db.addEventListener('click',function(e){
      e.stopPropagation();closeReactionPanel();
      if(msgType==='general')socket.emit('deleteMessage',msgId);
      else if(msgType==='pm')socket.emit('deletePrivateMessage',msgId);
      else if(msgType==='room')socket.emit('deleteRoomMessage',{roomId:currentRoomId,msgId:msgId});
    });
    panel.appendChild(db);
  }
  document.body.appendChild(panel);activePanel=panel;
  var rect=anchorEl.getBoundingClientRect();
  var pw=panel.offsetWidth||380;
  var left=rect.left+(rect.width/2)-(pw/2);
  if(left<8)left=8;if(left+pw>window.innerWidth-8)left=window.innerWidth-pw-8;
  var top=rect.top-66;if(top<8)top=rect.bottom+8;
  panel.style.left=left+'px';panel.style.top=top+'px';
  setTimeout(function(){
    function outsideClose1(e){if(activePanel&&!activePanel.contains(e.target)){closeReactionPanel();document.removeEventListener('pointerdown',outsideClose1);}}
    document.addEventListener('pointerdown',outsideClose1);
  },100);
}
window.showReactionPanel=showReactionPanel;

/* ── REACTIONS UPDATE ── */
socket.on('reactionUpdated',function(d){
  var el=document.getElementById('msg-'+d.msgId)||document.getElementById('pm-'+d.msgId)||document.getElementById('rmsg-'+d.msgId);
  if(!el)return;
  var row=el.querySelector('.reactions-row');
  if(!row){row=document.createElement('div');row.className='reactions-row';el.appendChild(row);}
  row.innerHTML='';
  d.reactions.forEach(function(r){
    var btn=document.createElement('button');btn.className='reaction-btn';
    btn.innerHTML=r.emoji+' <span class="rc">'+r.count+'</span>';
    btn.onclick=function(){socket.emit('addReaction',{msgType:d.msgType,msgId:d.msgId,emoji:r.emoji});};
    row.appendChild(btn);
  });
});

function buildReplyQuote(msg){
  if(!msg.reply_to_id)return'';
  return'<div class="reply-quote"><div class="rq-name">'+escHtml(msg.reply_to_user||'')+'</div><div class="rq-text">'+escHtml(msg.reply_to_text||'📎 Медиа')+'</div></div>';
}

/* ── ATTACH SWIPE + TAP EVENTS ── */
function attachMsgEvents(div,msgId,msgType,authorName,textContent){
  var isMine=div.classList.contains('mine');
  var wrapper=document.createElement('div');
  wrapper.className='msg-wrapper '+(isMine?'mine':'other');
  var icon=document.createElement('div');icon.className='swipe-reply-icon';icon.textContent='↩';
  if(isMine){wrapper.appendChild(icon);wrapper.appendChild(div);}
  else{wrapper.appendChild(div);wrapper.appendChild(icon);}
  div._wrapper=wrapper;

  // determine canDelete
  function canDelete(){
    if(myRole==='admin'||myRole==='moderator')return true;
    if(msgType==='general')return authorName===myNickname;
    if(msgType==='pm')return isMine;
    if(msgType==='room')return isMine||(currentRoomData&&currentRoomData.myRole==='admin');
    return false;
  }

  // Swipe
  var touchStartX=0,touchStartY=0,swiping=false,swipeDone=false;
  div.addEventListener('touchstart',function(e){touchStartX=e.touches[0].clientX;touchStartY=e.touches[0].clientY;swiping=false;swipeDone=false;},{passive:true});
  div.addEventListener('touchmove',function(e){
    var dx=e.touches[0].clientX-touchStartX,dy=e.touches[0].clientY-touchStartY;
    if(Math.abs(dy)>Math.abs(dx)+10)return;
    var move=dx*(isMine?-1:1);if(move<0)return;
    swiping=true;
    div.style.transform='translateX('+Math.min(move,80)*(isMine?-1:1)+'px)';
    icon.classList.toggle('visible',move>=30);
    if(move>=60&&!swipeDone){swipeDone=true;if(navigator.vibrate)navigator.vibrate(30);}
    e.preventDefault();
  },{passive:false});
  div.addEventListener('touchend',function(){
    div.style.transition='transform 0.2s ease';div.style.transform='translateX(0)';icon.classList.remove('visible');
    setTimeout(function(){div.style.transition='';},200);
    if(swipeDone)setReply(msgId,authorName,textContent);
    swiping=false;
  },{passive:true});

  // Click/tap
  var clickTimer=null;
  div.addEventListener('click',function(e){
    if(swiping||swipeDone)return;
    if(e.target.closest('.del-btn,.reply-quote,.reaction-btn,.reactions-row,.msg-nick'))return;
    clearTimeout(clickTimer);
    clickTimer=setTimeout(function(){showReactionPanel(msgId,msgType,div,authorName,textContent,canDelete());},160);
  });
  div.addEventListener('contextmenu',function(e){
    e.preventDefault();showReactionPanel(msgId,msgType,div,authorName,textContent,canDelete());
  });
}

function addMsg(msg){
  var div=document.createElement('div');var isS=msg.username==='⚡ Система',isM=msg.username===myNickname;
  div.className='message '+(isS?'system':(isM?'mine':'other'));div.id='msg-'+msg.id;
  var html='';
  if(msg.user_login&&msg.vip_emoji)vipEmojiCache[msg.user_login]=msg.vip_emoji;
  if(!isM&&!isS)html+='<div class="name msg-nick" data-login="'+escHtml(msg.user_login||'')+'" onclick="onMsgNickClick(this)">'+getNickWithEmoji(msg.user_login||'',msg.username,msg.vip_emoji)+'</div>';
  html+=buildReplyQuote(msg);
  if(msg.type==='image'&&msg.image){
    var imgs=(function(){var p=parseImgField(msg.image);return Array.isArray(p)?p:[p];})();
    var msgId=msg.id||msg._id||'';
    if(imgs.length===1){
      html+='<div class="photo-img-wrap" data-msgs="'+encodeURIComponent(JSON.stringify(imgs))+'" data-msgid="'+msgId+'" data-idx="0" onclick="pvClickHandler(this,event)"><img src="'+imgs[0]+'" class="chat-photo" alt="фото"></div>';
    } else {
      var cnt=Math.min(imgs.length,4);
      html+='<div class="photo-grid count-'+cnt+'" data-msgs="'+encodeURIComponent(JSON.stringify(imgs))+'" data-msgid="'+msgId+'">';
      for(var pi=0;pi<cnt;pi++){html+='<img src="'+imgs[pi]+'" class="chat-photo" alt="фото" data-idx="'+pi+'" onclick="pvClickGridHandler(this.parentNode,pi,event)">'; }
      html+='</div>';
    }
  }
  else if(msg.type==='video_note'&&msg.voice){}
  else if(msg.type==='voice'&&msg.voice)html+='<audio controls src="'+msg.voice+'"></audio>';
  else if(msg.type==='sticker')html+='<span style="font-size:52px;line-height:1.2;display:block;">'+escHtml(msg.text||'')+'</span>';
  else if(msg.type==='photo_sticker'&&msg.image)html+='<img src="'+msg.image+'" style="max-width:160px;max-height:160px;border-radius:18px;display:block;margin:2px 0;cursor:pointer;" onclick="window.pvSingle&&pvSingle(msg.image)">';
  else if(msg.type==='video'&&(msg.file_url||msg.image))html+='<video src="'+(msg.file_url||msg.image)+'" controls style="max-width:260px;max-height:200px;border-radius:10px;display:block;margin-top:4px;"></video>'+(msg.file_name?'<div style="font-size:11px;color:var(--text-muted);margin-top:2px;">📹 '+escHtml(msg.file_name)+'</div>':'');
  else if(msg.type==='file'&&msg.file_url){var fsz2=msg.file_size?' ('+Math.round(msg.file_size/1024)+' KB)':'';html+='<a href="'+msg.file_url+'" download="'+(msg.file_name||'file')+'" style="display:flex;align-items:center;gap:8px;background:var(--bg-input);padding:10px 14px;border-radius:10px;color:var(--text-primary);text-decoration:none;margin-top:4px;max-width:260px;"><span style="font-size:24px;">📄</span><div><div style="font-size:13px;font-weight:600;">'+escHtml(msg.file_name||'Файл')+'</div><div style="font-size:11px;color:var(--text-muted);">'+fsz2+'</div></div></a>';}
  else if(msg.type==='location'&&msg.text){var geoHtml=buildLocationHtml(msg.text);html+=geoHtml||'<div class="text">'+formatText(msg.text||'')+'</div>';}
  else html+='<div class="text">'+formatText(msg.text||'')+'</div>';
  if(!isS)html+=buildMsgTime(msg.timestamp);
  div.innerHTML=html;
  if(msg.type==='video_note'&&msg.voice){div.classList.add('video-note-msg');div.appendChild(renderVideoNote(msg.voice,isM,msg.id));}
  attachMsgEvents(div,msg.id,'general',msg.username,msg.text||'');
  document.getElementById('messages').appendChild(div._wrapper||div);
}
function delMsg(id){socket.emit('deleteMessage',id);}
window.delMsg=delMsg;

/* ── SEND ── */


// ═══════════════════════════════════════════════
// MESSAGE SEARCH (Ctrl+F in chat)
// ═══════════════════════════════════════════════
var _searchMatches = [], _searchIdx = -1;

function openMsgSearch() {
  var bar = document.getElementById('msgSearchBar');
  if (!bar) return;
  bar.classList.add('visible');
  document.getElementById('msgSearchInput').value = '';
  document.getElementById('msgSearchInput').focus();
  _searchMatches = []; _searchIdx = -1;
  document.getElementById('msgSearchCount').textContent = '';
}
window.openMsgSearch = openMsgSearch;

function closeMsgSearch() {
  var bar = document.getElementById('msgSearchBar');
  if (bar) bar.classList.remove('visible');
  // Remove all highlights
  document.querySelectorAll('.msg-highlight').forEach(function(el) {
    el.outerHTML = el.textContent;
  });
  _searchMatches = []; _searchIdx = -1;
  document.getElementById('msgSearchCount').textContent = '';
}
window.closeMsgSearch = closeMsgSearch;

function msgSearchInput() {
  var q = document.getElementById('msgSearchInput').value.trim().toLowerCase();
  // Remove old highlights
  document.querySelectorAll('.msg-highlight').forEach(function(el) {
    el.outerHTML = el.textContent;
  });
  _searchMatches = []; _searchIdx = -1;
  if (!q) { document.getElementById('msgSearchCount').textContent = ''; return; }
  
  // Find all text nodes in .text divs
  var textEls = document.querySelectorAll('#messages .text, #messages .poll-question');
  textEls.forEach(function(el) {
    var txt = el.textContent;
    var idx = txt.toLowerCase().indexOf(q);
    if (idx === -1) return;
    // Highlight occurrences
    var result = '';
    var last = 0;
    var re = new RegExp(escHtml(q).replace(/[-[\]{}()*+?.,\\\\^$|#]/g,'\\\\$&'), 'gi');
    result = el.innerHTML.replace(new RegExp('('+q.replace(/[-[\]{}()*+?.,\\\\^$|#]/g,'\\\\$&')+')', 'gi'), '<mark class="msg-highlight">$1</mark>');
    el.innerHTML = result;
  });
  
  _searchMatches = Array.from(document.querySelectorAll('.msg-highlight'));
  document.getElementById('msgSearchCount').textContent = _searchMatches.length > 0 ? '1/' + _searchMatches.length : 'Не найдено';
  if (_searchMatches.length > 0) {
    _searchIdx = 0;
    _searchMatches[0].classList.add('active');
    _searchMatches[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
}
window.msgSearchInput = msgSearchInput;

function msgSearchNav(dir) {
  if (!_searchMatches.length) return;
  _searchMatches[_searchIdx].classList.remove('active');
  _searchIdx = (_searchIdx + dir + _searchMatches.length) % _searchMatches.length;
  _searchMatches[_searchIdx].classList.add('active');
  _searchMatches[_searchIdx].scrollIntoView({ behavior: 'smooth', block: 'center' });
  document.getElementById('msgSearchCount').textContent = (_searchIdx+1) + '/' + _searchMatches.length;
}
window.msgSearchNav = msgSearchNav;

// Ctrl+F to open search
document.addEventListener('keydown', function(e) {
  if ((e.ctrlKey||e.metaKey) && e.key === 'f' && document.getElementById('mainChat').classList.contains('show')) {
    e.preventDefault();
    var bar = document.getElementById('msgSearchBar');
    if (bar && bar.classList.contains('visible')) closeMsgSearch();
    else openMsgSearch();
  }
  if (e.key === 'Escape') closeMsgSearch();
});





// ═══════════════════════════════════════════════
// AI BOT PANEL
// ═══════════════════════════════════════════════
var _aiHistory = []; // {role, content}
var _aiTyping = false;

function toggleAiPanel() {
  var panel = document.getElementById('aiPanel');
  if (!panel) return;
  panel.classList.toggle('open');
  if (panel.classList.contains('open')) {
    document.getElementById('aiInput').focus();
  }
}
window.toggleAiPanel = toggleAiPanel;

function sendAiMessage() {
  var input = document.getElementById('aiInput');
  if (!input) return;
  var text = input.value.trim();
  if (!text || _aiTyping) return;
  input.value = '';
  
  var msgs = document.getElementById('aiMessages');
  // Add user message
  var userMsg = document.createElement('div');
  userMsg.className = 'ai-msg user';
  userMsg.textContent = text;
  msgs.appendChild(userMsg);
  
  // Add typing indicator
  var typingEl = document.createElement('div');
  typingEl.className = 'ai-msg bot typing';
  typingEl.id = 'aiTyping';
  typingEl.textContent = '🤖 печатает...';
  msgs.appendChild(typingEl);
  msgs.scrollTop = msgs.scrollHeight;
  
  _aiTyping = true;
  _aiHistory.push({ role: 'user', content: text });
  socket.emit('askAiBot', { message: text, history: _aiHistory.slice(-10) });
}
window.sendAiMessage = sendAiMessage;

// AI bot message handling is merged into the main newPrivateMessage handler above
// (handled via msg.from_login === '_ai_bot' check)
function handleAiBotMessage(msg) {
  var typingEl = document.getElementById('aiTyping');
  if (typingEl) typingEl.remove();
  _aiTyping = false;
  var msgs = document.getElementById('aiMessages');
  if (msgs) {
    var botMsg = document.createElement('div');
    botMsg.className = 'ai-msg bot';
    botMsg.innerHTML = formatText(msg.text || '');
    msgs.appendChild(botMsg);
    msgs.scrollTop = msgs.scrollHeight;
  }
  _aiHistory.push({ role: 'assistant', content: msg.text || '' });
}

// ═══════════════════════════════════════════════
// SHARE GEOLOCATION
// ═══════════════════════════════════════════════
function shareLocation() {
  if (!navigator.geolocation) return alert('Геолокация не поддерживается браузером');
  if (!currentView || currentView==='private'||currentView==='rooms'||currentView==='all') return;
  navigator.geolocation.getCurrentPosition(function(pos) {
    var lat = pos.coords.latitude.toFixed(6);
    var lon = pos.coords.longitude.toFixed(6);
    var text = '📍 Геолокация: ' + lat + ',' + lon;
    var rd = replyTo ? {reply_to_id:replyTo.id,reply_to_text:replyTo.text,reply_to_user:replyTo.user} : {};
    var payload = Object.assign({ text: text, type: 'location' }, rd);
    if (currentView==='pm' && currentPrivateLogin) socket.emit('privateMessage', Object.assign({ toLogin: currentPrivateLogin }, payload));
    else if (currentView==='room' && currentRoomId) socket.emit('roomMessage', Object.assign({ roomId: currentRoomId }, payload));
    else if (currentView==='general') socket.emit('chatMessage', payload);
    clearReply();
  }, function(err) {
    alert('Не удалось получить геолокацию: ' + err.message);
  }, { enableHighAccuracy: true, timeout: 10000 });
}
window.shareLocation = shareLocation;

// Override addPM/addMsg/addRoomMsg to handle location type
// We patch the type check in the message renderers via a shared helper
function buildLocationHtml(text) {
  var match = text && text.match(/📍 Геолокация: ([-\d.]+),([-\d.]+)/);
  if (!match) return null;
  var lat = match[1], lon = match[2];
  var mapUrl = 'https://static-maps.yandex.ru/1.x/?lang=ru_RU&ll='+lon+','+lat+'&z=15&size=260,140&l=map&pt='+lon+','+lat+',pm2rdm';
  var mapsLink = 'https://maps.google.com/?q='+lat+','+lon;
  return '<div class="geo-msg"><img class="geo-map-preview" src="'+mapUrl+'" alt="Карта" onerror="this.style.display=\'none\'"><div class="geo-info">📍 <a href="'+mapsLink+'" target="_blank" rel="noopener">Открыть на карте</a><div style="font-size:11px;color:var(--text-muted);margin-top:2px;">'+lat+', '+lon+'</div></div></div>';
}
window.buildLocationHtml = buildLocationHtml;

// ── Toast notifications ───────────────────────────────
function showToast(msg, dur){
  var t=document.getElementById('_toast');
  if(!t){t=document.createElement('div');t.id='_toast';t.className='toast-msg';document.body.appendChild(t);}
  t.textContent=msg;
  clearTimeout(t._timer);
  // Force reflow
  t.classList.remove('show');
  void t.offsetWidth;
  t.classList.add('show');
  t._timer=setTimeout(function(){t.classList.remove('show');},dur||3000);
}
window.showToast=showToast;


// ── Media lazy loading with IntersectionObserver ──────
var _mediaObserver = null;
(function initMediaObserver(){
  if(!('IntersectionObserver' in window)) return;
  _mediaObserver = new IntersectionObserver(function(entries){
    entries.forEach(function(entry){
      if(!entry.isIntersecting) return;
      var el = entry.target;
      var src = el.dataset.lazySrc;
      if(!src) return;
      if(el.tagName === 'IMG'){
        el.src = src;
        el.removeAttribute('data-lazy-src');
        el.classList.remove('lazy-placeholder');
      } else if(el.tagName === 'VIDEO'){
        el.src = src;
        el.removeAttribute('data-lazy-src');
        el.load();
      }
      _mediaObserver.unobserve(el);
    });
  }, { rootMargin: '200px' });
})();

function lazyObserve(el) {
  if(_mediaObserver && el.dataset.lazySrc) _mediaObserver.observe(el);
}

// Call after messages rendered
function observeAllLazy(){
  document.querySelectorAll('[data-lazy-src]').forEach(function(el){ lazyObserve(el); });
}


// ═══════════════════════════════════════════════
// SCHEDULED MESSAGES CLIENT
// ═══════════════════════════════════════════════
var _scheduledMsgs = [];

function openScheduleModal() {
  if (!currentView || currentView==='private'||currentView==='rooms'||currentView==='all') return;
  var text = document.getElementById('msgInput').value.trim();
  var now = new Date(Date.now() + 5*60000);
  var dtLocal = new Date(now.getTime() - now.getTimezoneOffset()*60000).toISOString().slice(0,16);
  socket.emit('getScheduledMessages');
  
  var modal = document.createElement('div');
  modal.className = 'sched-modal';
  modal.id = 'schedModal';
  modal.innerHTML = '<div class="sched-modal-box"><h3>⏰ Отложенная отправка</h3>' +
    '<textarea id="schedText" rows="3" placeholder="Текст сообщения..." style="background:var(--bg-input);border:1px solid var(--border-color);border-radius:10px;padding:10px;color:var(--text-primary);font-size:14px;width:100%;box-sizing:border-box;resize:none;outline:none;">' + escHtml(text) + '</textarea>' +
    '<input type="datetime-local" id="schedTime" value="'+dtLocal+'">' +
    '<div id="schedList" class="sched-list"></div>' +
    '<div style="display:flex;gap:8px;justify-content:flex-end">' +
    '<button onclick="document.getElementById(\'schedModal\').remove()" style="background:var(--bg-input);border:none;border-radius:10px;padding:9px 18px;color:var(--text-secondary);cursor:pointer;font-weight:600">Закрыть</button>' +
    '<button onclick="submitSchedule()" style="background:var(--accent);border:none;border-radius:10px;padding:9px 18px;color:#fff;cursor:pointer;font-weight:600">Запланировать</button>' +
    '</div></div>';
  document.body.appendChild(modal);
  modal.addEventListener('click', function(e){ if(e.target===modal) modal.remove(); });
}
window.openScheduleModal = openScheduleModal;

function renderScheduledList() {
  var list = document.getElementById('schedList');
  if (!list) return;
  if (_scheduledMsgs.length === 0) { list.innerHTML = '<div style="color:var(--text-muted);font-size:13px;text-align:center;padding:8px">Нет запланированных сообщений</div>'; return; }
  list.innerHTML = '<div style="font-size:12px;color:var(--text-muted);margin-bottom:4px">Запланированные:</div>' +
    _scheduledMsgs.map(function(m){
      var dt = new Date(m.send_at).toLocaleString('ru');
      return '<div class="sched-item"><span class="sched-item-text">'+escHtml(m.text||'')+'</span><span class="sched-item-time">'+dt+'</span><button class="sched-cancel-btn" onclick="cancelScheduled('+m.id+')" title="Отменить">✕</button></div>';
    }).join('');
}

function submitSchedule() {
  var text = document.getElementById('schedText').value.trim();
  var dt = document.getElementById('schedTime').value;
  if (!text) return alert('Введите текст');
  if (!dt) return alert('Выберите время');
  var sendAt = new Date(dt).getTime();
  if (sendAt <= Date.now()) return alert('Время должно быть в будущем');
  var chatType = currentView;
  var chatId = currentView==='pm' ? currentPrivateLogin : (currentView==='room' ? currentRoomId : 'general');
  socket.emit('scheduleMessage', { chatType, chatId, text, sendAt });
  document.getElementById('schedModal').remove();
  document.getElementById('msgInput').value = '';
}
window.submitSchedule = submitSchedule;

function cancelScheduled(id) {
  socket.emit('cancelScheduledMessage', { id });
}
window.cancelScheduled = cancelScheduled;

socket.on('scheduledMessages', function(msgs) {
  _scheduledMsgs = msgs;
  renderScheduledList();
});

socket.on('scheduledMsgCreated', function(m) {
  _scheduledMsgs.push(m);
  _scheduledMsgs.sort(function(a,b){ return a.send_at-b.send_at; });
  // Show toast
  var dt = new Date(m.sendAt).toLocaleString('ru');
  var t = document.getElementById('_rateLimitToast');
  if(!t){t=document.createElement('div');t.id='_rateLimitToast';t.style.cssText='position:fixed;bottom:80px;left:50%;transform:translateX(-50%);background:#27ae60;color:#fff;padding:10px 20px;border-radius:10px;font-size:14px;font-weight:600;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,0.4);transition:opacity 0.3s;';document.body.appendChild(t);}
  t.style.background='#27ae60';t.textContent='⏰ Сообщение запланировано на '+dt;t.style.opacity='1';
  clearTimeout(t._ht);t._ht=setTimeout(function(){t.style.opacity='0';},3000);
});

socket.on('scheduledMsgCancelled', function(d) {
  _scheduledMsgs = _scheduledMsgs.filter(function(m){ return m.id !== d.id; });
  renderScheduledList();
});

socket.on('scheduledMsgSent', function(d) {
  _scheduledMsgs = _scheduledMsgs.filter(function(m){ return m.id !== d.id; });
});

// ═══════════════════════════════════════════════
// SECRET CHATS - AUTO DELETE TIMER
// ═══════════════════════════════════════════════
var _secretTimers = {}; // login -> seconds

function openSecretTimerModal() {
  if (currentView !== 'pm' || !currentPrivateLogin) return;
  var current = _secretTimers[currentPrivateLogin] || 0;
  var opts = [
    {label:'Выкл', val:0}, {label:'30 сек', val:30}, {label:'1 мин', val:60},
    {label:'5 мин', val:300}, {label:'1 час', val:3600}, {label:'1 день', val:86400}
  ];
  var modal = document.createElement('div');
  modal.className = 'secret-modal';
  modal.innerHTML = '<div class="secret-modal-box"><h3>🔥 Самоудаляющиеся сообщения</h3><p style="font-size:13px;color:var(--text-secondary);margin:0">Сообщения в этом чате будут автоматически удаляться через выбранное время после отправки.</p>' +
    '<div class="secret-timer-options">' +
    opts.map(function(o){ return '<div class="secret-timer-opt'+(current===o.val?' selected':'')+'" onclick="setSecretTimer('+o.val+',this)">'+o.label+'</div>'; }).join('') +
    '</div><div style="display:flex;gap:8px;justify-content:flex-end"><button onclick="this.closest(\'.secret-modal\').remove()" style="background:var(--bg-input);border:none;border-radius:10px;padding:9px 18px;color:var(--text-secondary);cursor:pointer;font-weight:600">Закрыть</button></div></div>';
  document.body.appendChild(modal);
  modal.addEventListener('click', function(e){ if(e.target===modal) modal.remove(); });
}
window.openSecretTimerModal = openSecretTimerModal;

function setSecretTimer(seconds, el) {
  document.querySelectorAll('.secret-timer-opt').forEach(function(o){ o.classList.remove('selected'); });
  el.classList.add('selected');
  socket.emit('setSecretTimer', { otherLogin: currentPrivateLogin, timerSeconds: seconds });
  setTimeout(function(){ document.querySelector('.secret-modal')?.remove(); }, 300);
}
window.setSecretTimer = setSecretTimer;

function updateSecretBadge(login, seconds) {
  _secretTimers[login] = seconds;
  if (currentView !== 'pm' || currentPrivateLogin !== login) return;
  // Update or create badge in header
  var existing = document.getElementById('secretTimerBadge');
  var sub = document.getElementById('chatSubtitle');
  if (!sub) return;
  if (seconds > 0) {
    var label = seconds < 60 ? seconds+'с' : seconds < 3600 ? Math.round(seconds/60)+'мин' : seconds < 86400 ? Math.round(seconds/3600)+'ч' : Math.round(seconds/86400)+'д';
    if (!existing) {
      var badge = document.createElement('span');
      badge.id = 'secretTimerBadge';
      badge.className = 'secret-timer-badge';
      badge.onclick = openSecretTimerModal;
      sub.insertBefore(badge, sub.firstChild);
      existing = badge;
    }
    existing.innerHTML = '🔥 ' + label;
  } else {
    if (existing) existing.remove();
  }
}

socket.on('secretTimerSet', function(d) {
  updateSecretBadge(d.otherLogin, d.timerSeconds);
});

socket.on('secretTimerResult', function(d) {
  updateSecretBadge(d.otherLogin, d.timerSeconds);
});

socket.on('secretMsgDeleted', function(d) {
  var el = document.getElementById('pm-'+d.id);
  if (el) {
    el.style.transition = 'opacity 0.5s';
    el.style.opacity = '0';
    setTimeout(function(){ var w = el._wrapper || el; if(w.parentNode) w.parentNode.removeChild(w); }, 500);
  }
});

// ═══════════════════════════════════════════════
// POLLS CLIENT
// ═══════════════════════════════════════════════
var _polls = {}; // pollId -> pollData

function openCreatePoll() {
  if (!currentView || currentView === 'private' || currentView === 'rooms' || currentView === 'all') return;
  var modal = document.createElement('div');
  modal.className = 'poll-modal';
  modal.id = 'pollCreateModal';
  modal.innerHTML = `
    <div class="poll-modal-box">
      <h3>📊 Создать опрос</h3>
      <input id="pollQuestion" placeholder="Вопрос..." maxlength="300">
      <div id="pollOptions">
        <div class="poll-option-row"><input class="poll-opt-inp" placeholder="Вариант 1" maxlength="100"><button class="poll-remove-opt" onclick="removePollOpt(this)">✕</button></div>
        <div class="poll-option-row"><input class="poll-opt-inp" placeholder="Вариант 2" maxlength="100"><button class="poll-remove-opt" onclick="removePollOpt(this)">✕</button></div>
      </div>
      <button class="poll-add-opt" onclick="addPollOpt()">+ Добавить вариант</button>
      <div class="poll-toggle-row">
        <label><input type="checkbox" id="pollMultiple"> Несколько ответов</label>
        <label><input type="checkbox" id="pollAnon"> Анонимный</label>
      </div>
      <div class="poll-modal-btns">
        <button class="poll-cancel-btn" onclick="document.getElementById('pollCreateModal').remove()">Отмена</button>
        <button class="poll-create-btn" onclick="submitCreatePoll()">Создать</button>
      </div>
    </div>`;
  document.body.appendChild(modal);
  modal.addEventListener('click', function(e){ if(e.target===modal) modal.remove(); });
  document.getElementById('pollQuestion').focus();
}
window.openCreatePoll = openCreatePoll;

function addPollOpt() {
  var opts = document.getElementById('pollOptions');
  if (opts.children.length >= 10) return;
  var n = opts.children.length + 1;
  var row = document.createElement('div');
  row.className = 'poll-option-row';
  row.innerHTML = '<input class="poll-opt-inp" placeholder="Вариант ' + n + '" maxlength="100"><button class="poll-remove-opt" onclick="removePollOpt(this)">✕</button>';
  opts.appendChild(row);
}
window.addPollOpt = addPollOpt;

function removePollOpt(btn) {
  var opts = document.getElementById('pollOptions');
  if (opts.children.length <= 2) return;
  btn.parentElement.remove();
}
window.removePollOpt = removePollOpt;

function submitCreatePoll() {
  var question = document.getElementById('pollQuestion').value.trim();
  if (!question) return;
  var inputs = document.querySelectorAll('.poll-opt-inp');
  var options = Array.from(inputs).map(function(i){ return i.value.trim(); }).filter(Boolean);
  if (options.length < 2) return alert('Добавьте минимум 2 варианта');
  var multiple = document.getElementById('pollMultiple').checked;
  var anon = document.getElementById('pollAnon').checked;
  var chatType = currentView; // 'general', 'pm', 'room'
  var chatId = currentView==='pm' ? currentPrivateLogin : (currentView==='room' ? currentRoomId : 'general');
  socket.emit('createPoll', { chatType, chatId, question, options, multipleChoice: multiple, anonymous: anon });
  document.getElementById('pollCreateModal').remove();
}
window.submitCreatePoll = submitCreatePoll;

function renderPollCard(poll, container) {
  var existing = document.getElementById('poll-'+poll.id);
  var isNew = !existing;
  var el = existing || document.createElement('div');
  el.className = 'poll-card';
  el.id = 'poll-'+poll.id;
  
  var totalVoters = Object.keys(poll.votes && !poll.votes.__anonymous ? poll.votes : {}).length;
  var myVote = poll.votes ? poll.votes[myLogin] : null;
  
  // Count votes per option
  var voteCounts = {};
  if (poll.votes && poll.votes.__anonymous) {
    voteCounts = poll.votes.counts || {};
    totalVoters = Object.values(voteCounts).reduce(function(a,b){return a+b;}, 0);
  } else {
    Object.values(poll.votes||{}).forEach(function(v) {
      var ids = Array.isArray(v) ? v : [v];
      ids.forEach(function(id){ voteCounts[id] = (voteCounts[id]||0) + 1; });
    });
  }
  
  var optsHtml = (poll.options||[]).map(function(opt) {
    var count = voteCounts[opt.id] || 0;
    var pct = totalVoters > 0 ? Math.round(count/totalVoters*100) : 0;
    var voted = myVote !== null && myVote !== undefined && (Array.isArray(myVote) ? myVote.includes(opt.id) : myVote===opt.id);
    return '<div class="poll-option" onclick="votePoll('+poll.id+','+opt.id+')">' +
      '<div class="poll-option-bar-wrap'+(voted?' voted':'')+'">' +
        '<div class="poll-option-bar" style="width:'+pct+'%"></div>' +
        '<div class="poll-option-label"><span>'+escHtml(opt.text)+(voted?' ✓':'')+'</span><span class="poll-option-pct">'+count+' ('+pct+'%)</span></div>' +
      '</div></div>';
  }).join('');
  
  var canDelete = poll.creatorLogin===myLogin || myRole==='admin';
  el.innerHTML = '<div class="poll-question">📊 '+escHtml(poll.question)+'</div>' + optsHtml +
    '<div class="poll-footer"><span>'+totalVoters+' голосов'+(poll.anonymous?' · анонимный':'')+(poll.multipleChoice?' · несколько':'')+' · от '+escHtml(poll.creatorNick||poll.creatorLogin||'')+'</span>' +
    (canDelete ? '<button class="poll-delete-btn" onclick="deletePoll('+poll.id+')">🗑</button>' : '') + '</div>';
  
  if (isNew && container) container.appendChild(el);
}

function votePoll(pollId, optionId) {
  socket.emit('votePoll', { pollId, optionId });
}
window.votePoll = votePoll;

function deletePoll(pollId) {
  if (!confirm('Удалить опрос?')) return;
  socket.emit('deletePoll', { pollId });
}
window.deletePoll = deletePoll;

socket.on('newPoll', function(poll) {
  _polls[poll.id] = poll;
  // Only show if in correct chat
  var isMyChat = false;
  if (poll.chatType==='general' && currentView==='general') isMyChat = true;
  if (poll.chatType==='room' && currentView==='room' && currentRoomId==poll.chatId) isMyChat = true;
  if (poll.chatType==='pm' && currentView==='pm' && currentPrivateLogin===poll.chatId) isMyChat = true;
  if (isMyChat) {
    var md = document.getElementById('messages');
    renderPollCard(poll, md);
    md.scrollTop = md.scrollHeight;
  }
});

socket.on('pollVoteUpdate', function(d) {
  if (!_polls[d.pollId]) return;
  _polls[d.pollId].votes = d.votes;
  renderPollCard(_polls[d.pollId], null);
});

socket.on('pollDeleted', function(d) {
  delete _polls[d.pollId];
  var el = document.getElementById('poll-'+d.pollId);
  if (el) el.remove();
});


// ── TEXT FORMATTING TOOLBAR ──────────────────────────────
function toggleFmtToolbar(){
  var tb=document.getElementById('fmtToolbar');
  tb.classList.toggle('visible');
  document.getElementById('fmtToggleBtn').style.color=tb.classList.contains('visible')?'var(--accent)':'';
}
function fmtWrap(before,after){
  var inp=document.getElementById('msgInput');
  var s=inp.selectionStart,e=inp.selectionEnd;
  var val=inp.value;
  var sel=val.slice(s,e)||'текст';
  inp.value=val.slice(0,s)+before+sel+after+val.slice(e);
  inp.selectionStart=s+before.length;
  inp.selectionEnd=s+before.length+sel.length;
  inp.focus();
}
// Ctrl+B / Ctrl+I shortcuts
document.addEventListener('keydown',function(e){
  if((e.ctrlKey||e.metaKey)&&document.getElementById('msgInput')===document.activeElement){
    if(e.key==='b'){e.preventDefault();fmtWrap('**','**');}
    if(e.key==='i'){e.preventDefault();fmtWrap('_','_');}
  }
});

document.getElementById('sendBtn').addEventListener('click',function(){
  var text=document.getElementById('msgInput').value.trim();if(!text)return;
  // Ghost Chat
  if(currentView==='ghost'){
    if(!_ghostRoomId){ return; }
    socket.emit('ghostMessage', { roomId: _ghostRoomId, text: text, anon: _ghostAnon });
    document.getElementById('msgInput').value='';
    return;
  }
  // Edit mode
  if(editingMsgId){
    socket.emit('editMessage',{msgType:editingMsgType,msgId:editingMsgId,newText:text});
    cancelEdit();
    return;
  }
  var rd=replyTo?{reply_to_id:replyTo.id,reply_to_text:replyTo.text,reply_to_user:replyTo.user}:{};
  if(currentView==='pm'&&currentPrivateLogin){socket.emit('privateMessage',Object.assign({toLogin:currentPrivateLogin,text:text,type:'text'},rd));socket.emit('privateStopTyping',currentPrivateLogin);}
  else if(currentView==='room'&&currentRoomId){socket.emit('roomMessage',Object.assign({roomId:currentRoomId,text:text,type:'text'},rd));socket.emit('roomStopTyping',currentRoomId);}
  else if(currentView==='general'){socket.emit('chatMessage',Object.assign({text:text,type:'text'},rd));socket.emit('stopTyping');}
  document.getElementById('msgInput').value='';clearReply();
});
document.getElementById('msgInput').addEventListener('keypress',function(e){if(e.key==='Enter')document.getElementById('sendBtn').click();});
document.getElementById('msgInput').addEventListener('input',function(){
  if(currentView==='pm'&&currentPrivateLogin)socket.emit('privateTyping',currentPrivateLogin);
  else if(currentView==='room'&&currentRoomId)socket.emit('roomTyping',currentRoomId);
  else if(currentView==='general')socket.emit('typing');
  clearTimeout(typingTimeout);
  typingTimeout=setTimeout(function(){
    if(currentView==='pm'&&currentPrivateLogin)socket.emit('privateStopTyping',currentPrivateLogin);
    else if(currentView==='room'&&currentRoomId)socket.emit('roomStopTyping',currentRoomId);
    else socket.emit('stopTyping');
  },2000);
});

/* ── PHOTO ── */
document.getElementById('photoInput').addEventListener('change',function(){
  var allFiles=Array.from(this.files);if(!allFiles.length)return;
  var self=this;
  var rd=replyTo?{reply_to_id:replyTo.id,reply_to_text:replyTo.text,reply_to_user:replyTo.user}:{};
  
  // Separate videos from images
  var videoFiles=allFiles.filter(function(f){return f.type.startsWith('video/');});
  var imageFiles=allFiles.filter(function(f){return f.type.startsWith('image/');});
  
  // Check image size limits
  var oversized=imageFiles.filter(function(f){return f.size>10*1024*1024;});
  if(oversized.length){alert('Макс 10MB на фото. Большие файлы пропущены.');imageFiles=imageFiles.filter(function(f){return f.size<=10*1024*1024;});}
  
  // Send videos one by one
  videoFiles.forEach(function(file){
    if(file.size>50*1024*1024){alert('Видео слишком большое (макс 50MB): '+file.name);return;}
    var reader=new FileReader();
    reader.onload=function(ev){
      var payload=Object.assign({type:'video',text:file.name,file_url:ev.target.result,file_name:file.name,file_size:file.size},rd);
      if(currentView==='ghost'&&_ghostRoomId){socket.emit('ghostMessage',Object.assign({roomId:_ghostRoomId,anon:_ghostAnon},payload));}
      else if(currentView==='pm'&&currentPrivateLogin)socket.emit('privateMessage',Object.assign({toLogin:currentPrivateLogin},payload));
      else if(currentView==='room'&&currentRoomId)socket.emit('roomMessage',Object.assign({roomId:currentRoomId},payload));
      else if(currentView==='general')socket.emit('chatMessage',payload);
    };
    reader.readAsDataURL(file);
  });
  
  // Send images - compress via Canvas first to keep size manageable
  if(imageFiles.length>0){
    function compressImage(file,cb){
      var reader=new FileReader();
      reader.onload=function(e){
        var img=new Image();
        img.onload=function(){
          var canvas=document.createElement('canvas');
          var MAX=1280;var w=img.width,h=img.height;
          if(w>MAX||h>MAX){if(w>h){h=Math.round(h*MAX/w);w=MAX;}else{w=Math.round(w*MAX/h);h=MAX;}}
          canvas.width=w;canvas.height=h;
          canvas.getContext('2d').drawImage(img,0,0,w,h);
          cb(canvas.toDataURL('image/jpeg',file.size>2*1024*1024?0.7:0.85));
        };
        img.onerror=function(){cb(e.target.result);};
        img.src=e.target.result;
      };
      reader.onerror=function(){cb(null);};
      reader.readAsDataURL(file);
    }
    function compressAll(list,cb){
      var results=[];var done=0;
      if(!list.length){cb([]);return;}
      list.forEach(function(f,i){compressImage(f,function(d){if(d)results[i]=d;done++;if(done===list.length)cb(results.filter(Boolean));});});
    }
    compressAll(imageFiles,function(images){
      if(!images.length)return;
      var payload=Object.assign({image:images.length===1?images[0]:images,type:'image'},rd);
      if(currentView==='ghost'&&_ghostRoomId){socket.emit('ghostMessage',Object.assign({roomId:_ghostRoomId,anon:_ghostAnon},payload));}
      else if(currentView==='pm'&&currentPrivateLogin)socket.emit('privateMessage',Object.assign({toLogin:currentPrivateLogin},payload));
      else if(currentView==='room'&&currentRoomId)socket.emit('roomMessage',Object.assign({roomId:currentRoomId},payload));
      else if(currentView==='general')socket.emit('chatMessage',payload);
    });
  }
  
  clearReply();self.value='';
});

/* ── VIP PHOTO EMOJI ── */
document.getElementById('vipPhotoInput').addEventListener('change',function(){
  var file=this.files[0];if(!file)return;
  var reader=new FileReader();
  reader.onload=function(e){
    // Crop to 40x40 circle via canvas
    var img=new Image();
    img.onload=function(){
      var canvas=document.createElement('canvas');canvas.width=40;canvas.height=40;
      var ctx=canvas.getContext('2d');
      ctx.beginPath();ctx.arc(20,20,20,0,Math.PI*2);ctx.clip();
      var s=Math.min(img.width,img.height);
      var ox=(img.width-s)/2,oy=(img.height-s)/2;
      ctx.drawImage(img,ox,oy,s,s,0,0,40,40);
      var dataUrl=canvas.toDataURL('image/jpeg',0.85);
      // Save as VIP emoji
      myVipEmoji=dataUrl;vipEmojiCache[myLogin]=dataUrl;
      if(window.socket)window.socket.emit('setVipEmoji',{emoji:dataUrl});
      openVipSettings();
    };
    img.src=e.target.result;
  };
  reader.readAsDataURL(file);
  this.value='';
});

/* ── VOICE ── */
var micBtn=document.getElementById('micBtn'),recordingBar=document.getElementById('recordingBar'),inputArea=document.getElementById('inputArea');
micBtn.addEventListener('click',function(){
  if(mediaRecorder&&mediaRecorder.state==='recording')return;
  navigator.mediaDevices.getUserMedia({audio:true}).then(function(stream){
    mediaRecorder=new MediaRecorder(stream);audioChunks=[];
    mediaRecorder.ondataavailable=function(e){audioChunks.push(e.data);};
    mediaRecorder.onstop=function(){stream.getTracks().forEach(function(t){t.stop();});};
    mediaRecorder.start();micBtn.classList.add('recording');inputArea.style.display='none';recordingBar.classList.add('active');
    recSeconds=0;document.getElementById('recTime').textContent='0:00';
    recInterval=setInterval(function(){recSeconds++;var m=Math.floor(recSeconds/60),s=recSeconds%60;document.getElementById('recTime').textContent=m+':'+(s<10?'0':'')+s;},1000);
  }).catch(function(){alert('Нет доступа к микрофону');});
});
document.getElementById('recCancel').addEventListener('click',function(){if(mediaRecorder&&mediaRecorder.state==='recording'){mediaRecorder.onstop=function(){};mediaRecorder.stop();}stopRec();});
document.getElementById('recSend').addEventListener('click',function(){
  if(mediaRecorder&&mediaRecorder.state==='recording'){
    mediaRecorder.onstop=function(){
      mediaRecorder.stream&&mediaRecorder.stream.getTracks().forEach(function(t){t.stop();});
      var blob=new Blob(audioChunks,{type:'audio/webm'});var reader=new FileReader();
      reader.onload=function(e){
        var rd=replyTo?{reply_to_id:replyTo.id,reply_to_text:replyTo.text,reply_to_user:replyTo.user}:{};
        if(currentView==='ghost'&&_ghostRoomId){socket.emit('ghostMessage',{roomId:_ghostRoomId,voice:e.target.result,type:'voice',anon:_ghostAnon});}
        else if(currentView==='pm'&&currentPrivateLogin)socket.emit('privateMessage',Object.assign({toLogin:currentPrivateLogin,voice:e.target.result,type:'voice'},rd));
        else if(currentView==='room'&&currentRoomId)socket.emit('roomMessage',Object.assign({roomId:currentRoomId,voice:e.target.result,type:'voice'},rd));
        else if(currentView==='general')socket.emit('chatMessage',Object.assign({voice:e.target.result,type:'voice'},rd));
        clearReply();
      };reader.readAsDataURL(blob);};mediaRecorder.stop();}stopRec();
});
function stopRec(){clearInterval(recInterval);micBtn.classList.remove('recording');recordingBar.classList.remove('active');inputArea.style.display='flex';}

/* ── CREATE ROOM ── */
document.getElementById('createRoomBtn').addEventListener('click',function(){document.getElementById('createRoomModal').classList.add('show');});
document.getElementById('createRoomCancel').addEventListener('click',function(){document.getElementById('createRoomModal').classList.remove('show');});
document.getElementById('createRoomSave').addEventListener('click',function(){
  var name=document.getElementById('createRoomName').value.trim(),type=document.getElementById('createRoomType').value;
  if(!name)return;socket.emit('createRoom',{name:name,type:type});document.getElementById('createRoomName').value='';document.getElementById('createRoomModal').classList.remove('show');
});
socket.on('roomCreated',function(r){openRoom(r.id);});

/* ── NICK / PASS MODALS ── */
document.getElementById('nickCancel').addEventListener('click',function(){document.getElementById('nickModal').classList.remove('show');});
document.getElementById('nickSave').addEventListener('click',function(){
  var n=document.getElementById('newNickInput').value.trim();
  if(!n){document.getElementById('nickError').textContent='Введи ник';return;}
  socket.emit('changeNickname',n);document.getElementById('nickModal').classList.remove('show');
});
document.getElementById('passCancel').addEventListener('click',function(){document.getElementById('passModal').classList.remove('show');});
document.getElementById('passSave').addEventListener('click',function(){
  var o=document.getElementById('oldPass').value.trim(),n=document.getElementById('newPass').value.trim(),n2=document.getElementById('newPass2').value.trim();
  document.getElementById('passError').textContent='';document.getElementById('passSuccess').textContent='';
  if(!o||!n||!n2){document.getElementById('passError').textContent='Заполни все поля';return;}
  if(n!==n2){document.getElementById('passError').textContent='Пароли не совпадают';return;}
  if(n.length<4){document.getElementById('passError').textContent='Минимум 4 символа';return;}
  socket.emit('changePassword',{oldPassword:o,newPassword:n});
});
socket.on('passwordResult',function(m){if(m==='ok'){document.getElementById('passSuccess').textContent='Пароль изменён!';setTimeout(function(){document.getElementById('passModal').classList.remove('show');},1500);}else document.getElementById('passError').textContent=m;});

document.getElementById('mobileBackBtn').addEventListener('click',function(){
  if(currentView==='ghost'){
    // Ghost chat — must destroy history for everyone
    var rid=_ghostRoomId;
    ghostCleanup();
    if(rid) socket.emit('ghostLeave',{roomId:rid});
    return;
  }
  document.getElementById('mainChat').classList.remove('show');document.getElementById('noChat').style.display='flex';document.getElementById('messages').innerHTML='';
  if(currentView==='pm'){currentView='private';currentPrivateLogin=null;emitGetMyChats();}
  else if(currentView==='room'){currentView='rooms';currentRoomId=null;currentRoomData=null;emitGetMyRooms();}
  else if(currentView==='general')switchToGeneral();
});

document.getElementById('settingsBtn').addEventListener('click',openSettings);

/* ── ADMIN ── */
document.getElementById('adminPanelBtn').addEventListener('click',function(){document.getElementById('adminOverlay').classList.add('show');loadTab('stats');});
document.getElementById('adminClose').addEventListener('click',function(){document.getElementById('adminOverlay').classList.remove('show');});
document.getElementById('adminTabs').addEventListener('click',function(e){if(e.target.dataset.tab){document.querySelectorAll('#adminTabs button').forEach(function(b){b.classList.remove('active');});e.target.classList.add('active');loadTab(e.target.dataset.tab);}});
socket.on('adminStats',function(s){document.getElementById('adminContent').innerHTML='<div class="stat-box"><div class="stat"><div class="num">'+s.users+'</div><div class="label">Юзеров</div></div><div class="stat"><div class="num">'+s.messages+'</div><div class="label">Сообщений</div></div><div class="stat"><div class="num">'+s.pms+'</div><div class="label">Личных</div></div><div class="stat"><div class="num">'+s.rooms+'</div><div class="label">Комнат</div></div><div class="stat"><div class="num">'+s.online+'</div><div class="label">Онлайн</div></div></div>';});
socket.on('adminUsers',function(users){
  var c=document.getElementById('adminContent');c.innerHTML='';
  users.forEach(function(u){
    var row=document.createElement('div');row.className='user-row';
    var infoDiv=document.createElement('div');infoDiv.className='uinfo';
    infoDiv.innerHTML='<b>'+u.nickname+'</b> ('+u.login+') <span class="role '+u.role+'">'+u.role+'</span>'+(u.banned?' 🚫':'')+(u.muted_until>Date.now()?' 🔇':'')+(u.vip_until>Date.now()?' 👑':'')+(u.verified?' ✓':'');
    row.appendChild(infoDiv);
    var actDiv=document.createElement('div');actDiv.className='uactions';
    if(u.banned){var b=document.createElement('button');b.className='admin-act-btn abtn-unban';b.textContent='Разбан';b.addEventListener('click',(function(l){return function(){socket.emit('adminUnbanUser',l);};})(u.login));actDiv.appendChild(b);}
    else{var b=document.createElement('button');b.className='admin-act-btn abtn-ban';b.textContent='Бан';b.addEventListener('click',(function(l){return function(){if(confirm('Забанить?'))socket.emit('adminBanUser',l);};})(u.login));actDiv.appendChild(b);}
    if(u.muted_until>Date.now()){
      var bum=document.createElement('button');bum.className='admin-act-btn abtn-unban';bum.textContent='Снять мут';bum.addEventListener('click',(function(l){return function(){socket.emit('adminUnmuteUser',l);};})(u.login));actDiv.appendChild(bum);
    } else {
      var bm=document.createElement('button');bm.className='admin-act-btn abtn-mute';bm.textContent='Мут';bm.addEventListener('click',(function(l){return function(){var m=prompt('Минут?','10');if(m)socket.emit('adminMuteUser',{login:l,minutes:parseInt(m)});};})(u.login));actDiv.appendChild(bm);
    }
    if(u.vip_until>Date.now()){
      var bvip=document.createElement('button');bvip.className='admin-act-btn abtn-mute';bvip.textContent='❌ VIP';bvip.addEventListener('click',(function(l){return function(){if(confirm('Снять VIP?'))socket.emit('adminRemoveVip',l);};})(u.login));actDiv.appendChild(bvip);
    }
    if(u.verified){
      var bver=document.createElement('button');bver.className='admin-act-btn abtn-mute';bver.textContent='❌ Верификация';bver.addEventListener('click',(function(l){return function(){if(confirm('Снять верификацию?'))socket.emit('adminRemoveVerify',l);};})(u.login));actDiv.appendChild(bver);
    }
    var bn=document.createElement('button');bn.className='admin-act-btn abtn-nick';bn.textContent='Ник';bn.addEventListener('click',(function(l){return function(){var n=prompt('Новый ник:');if(n)socket.emit('adminChangeNickname',{login:l,newNickname:n});};})(u.login));actDiv.appendChild(bn);
    var br=document.createElement('button');br.className='admin-act-btn abtn-role';br.textContent='Роль';br.addEventListener('click',(function(l){return function(){var r=prompt('Роль:','user');if(r)socket.emit('adminSetRole',{login:l,role:r});};})(u.login));actDiv.appendChild(br);
    var bd=document.createElement('button');bd.className='admin-act-btn abtn-del';bd.textContent='Удалить';bd.addEventListener('click',(function(l){return function(){if(confirm('Удалить?'))socket.emit('adminDeleteUser',l);};})(u.login));actDiv.appendChild(bd);
    row.appendChild(actDiv);c.appendChild(row);
  });
});
socket.on('adminLogs',function(logs){var html='';logs.forEach(function(l){var d=new Date(Number(l.timestamp));html+='<div class="log-row"><span class="time">'+d.toLocaleString()+'</span> <span class="act">'+l.action+'</span> '+l.username+' — '+l.detail+(l.ip?' ('+l.ip+')':'')+'</div>';});document.getElementById('adminContent').innerHTML=html||'<p>Логов нет</p>';});
socket.on('adminDone',function(msg){alert(msg);loadTab(currentAdminTab);});
socket.on('adminVipCodeCreated',function(d){
  alert('Код создан:\n'+d.code+'\n('+d.days+' дней)');
  loadTab('vip');
});
socket.on('adminVipCodes',function(rows){
  var el=document.getElementById('vipCodesList');if(!el)return;
  if(!rows.length){el.innerHTML='<div style="color:var(--text-muted);font-size:13px">Кодов ещё нет</div>';return;}
  el.innerHTML=rows.map(function(r){
    return '<div class="admin-vip-code"><span class="code-val">'+r.code+'</span>'
      +'<span>'+r.duration_days+' дн.</span>'
      +(r.used?'<span class="code-used">Использован: '+r.used_by+'</span>':'<span class="code-ok">Свободен</span>')
      +'</div>';
  }).join('');
});
function loadTab(tab){
  currentAdminTab=tab;var c=document.getElementById('adminContent');
  if(tab==='stats'){socket.emit('adminGetStats');c.innerHTML='Загрузка...';}
  else if(tab==='users'){socket.emit('adminGetUsers');c.innerHTML='Загрузка...';}
  else if(tab==='logs'){socket.emit('adminGetLogs');c.innerHTML='Загрузка...';}
  else if(tab==='announce'){c.innerHTML='<h3>📢 Объявление в общий чат</h3><div class="admin-input"><input id="annInput" placeholder="Текст..."><button id="annBtn">Отправить</button></div>';document.getElementById('annBtn').addEventListener('click',function(){var t=document.getElementById('annInput').value.trim();if(t){socket.emit('adminAnnounce',t);document.getElementById('annInput').value='';}});}
  else if(tab==='vip'){
    c.innerHTML='<div style="padding:16px"><h3 style="margin-bottom:14px">👑 VIP коды</h3><div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap"><input id="vipDaysInput" type="number" min="1" max="365" value="30" style="width:80px;padding:9px 12px;border:1.5px solid var(--border);border-radius:10px;background:var(--bg-input);color:var(--text-primary);outline:none;" placeholder="Дней"><button id="genVipBtn" style="padding:9px 16px;border:none;border-radius:10px;background:linear-gradient(135deg,#f7c948,#e8870a);color:#1a0a00;font-weight:700;cursor:pointer;">+ Создать код</button></div><div id="vipCodesList">Загрузка...</div></div>';
    document.getElementById('genVipBtn').addEventListener('click',function(){
      var days=parseInt(document.getElementById('vipDaysInput').value)||30;
      socket.emit('adminGenerateVipCode',{days});
    });
    socket.emit('adminGetVipCodes');
  }
  else if(tab==='danger'){c.innerHTML='<div class="danger-zone"><h4>⚠️ Опасная зона</h4><button id="clearBtn">🗑 Очистить чат</button><button id="delAllBtn">💀 Удалить все аккаунты</button></div>';document.getElementById('clearBtn').addEventListener('click',function(){if(confirm('Удалить ВСЕ сообщения?'))socket.emit('adminClearChat');});document.getElementById('delAllBtn').addEventListener('click',function(){if(confirm('Удалить ВСЕ аккаунты?'))if(confirm('ТОЧНО?'))socket.emit('adminDeleteAllUsers');});}
}

/* ── CALLS ── */
// Drag local video
const dragVideo=document.getElementById('localVideo');let isDragging=false,startX,startY,initialLeft,initialTop;
dragVideo.addEventListener('touchstart',startDrag,{passive:true});dragVideo.addEventListener('mousedown',startDrag);
function startDrag(e){isDragging=true;const evt=e.type==='touchstart'?e.touches[0]:e;startX=evt.clientX;startY=evt.clientY;const rect=dragVideo.getBoundingClientRect();initialLeft=rect.left;initialTop=rect.top;dragVideo.style.right='auto';dragVideo.style.bottom='auto';dragVideo.style.left=initialLeft+'px';dragVideo.style.top=initialTop+'px';document.addEventListener('touchmove',moveDrag,{passive:false});document.addEventListener('touchend',stopDrag);document.addEventListener('mousemove',moveDrag);document.addEventListener('mouseup',stopDrag);}
function moveDrag(e){if(!isDragging)return;const evt=e.type==='touchmove'?e.touches[0]:e;dragVideo.style.left=(initialLeft+evt.clientX-startX)+'px';dragVideo.style.top=(initialTop+evt.clientY-startY)+'px';e.preventDefault();}
function stopDrag(){isDragging=false;document.removeEventListener('touchmove',moveDrag);document.removeEventListener('touchend',stopDrag);document.removeEventListener('mousemove',moveDrag);document.removeEventListener('mouseup',stopDrag);}

// ═══════════════════════════════════════════════════════
// CALL SYSTEM
// ═══════════════════════════════════════════════════════
var localStream=null, remoteStream=null, peerConnection=null;
var incomingCallData=null;
var currentFacingMode='user';
var pendingIce=[], earlyCandidates=[];
var callPartnerLogin=null, callPartnerNick='', activeCallType='audio';
var callStartTime=0, callTimerInterval=null;
var incomingCallCountdown=null;
var _vibInterval=null;

const rtcConfig={
  iceServers:[
    {urls:'stun:stun.l.google.com:19302'},
    {urls:'stun:stun1.l.google.com:19302'},
    {urls:'stun:stun2.l.google.com:19302'},
    {urls:'stun:stun3.l.google.com:19302'},
    {urls:'stun:stun4.l.google.com:19302'},
    {urls:'stun:stun.stunprotocol.org:3478'},
    {urls:'stun:stun.voip.blackberry.com:3478'},
    {urls:'turn:openrelay.metered.ca:80',username:'openrelayproject',credential:'openrelayproject'},
    {urls:'turn:openrelay.metered.ca:443',username:'openrelayproject',credential:'openrelayproject'},
    {urls:'turn:openrelay.metered.ca:443?transport=tcp',username:'openrelayproject',credential:'openrelayproject'}
  ],
  iceCandidatePoolSize:10
};

// ── Таймер ────────────────────────────────────────────
function startCallTimer(){
  callStartTime=Date.now(); clearInterval(callTimerInterval);
  callTimerInterval=setInterval(function(){
    var sec=Math.floor((Date.now()-callStartTime)/1000);
    var m=Math.floor(sec/60),s=sec%60;
    var t=(m<10?'0':'')+m+':'+(s<10?'0':'')+s;
    var st=document.getElementById('callAudioStatus'); if(st) st.textContent=t;
    var mt=document.getElementById('callMiniTime'); if(mt) mt.textContent=t;
  },1000);
}
function stopCallTimer(){ clearInterval(callTimerInterval); callTimerInterval=null; callStartTime=0; }
function getDuration(){ return callStartTime ? Math.floor((Date.now()-callStartTime)/1000) : 0; }

// ── Вибрация ──────────────────────────────────────────
function startRinging(){
  stopRinging();
  if(navigator.vibrate){ _vibInterval=setInterval(function(){ navigator.vibrate([400,200,400]); },1200); }
}
function stopRinging(){
  if(_vibInterval){ clearInterval(_vibInterval); if(navigator.vibrate) navigator.vibrate(0); _vibInterval=null; }
}

// ── UI ────────────────────────────────────────────────
function showCallOverlay(){ document.getElementById('callOverlay').classList.add('show'); document.getElementById('callMiniBar').classList.remove('show'); }
function minimizeCall(){ document.getElementById('callOverlay').classList.remove('show'); document.getElementById('callMiniBar').classList.add('show'); }
function expandCall(){ showCallOverlay(); }
window.minimizeCall=minimizeCall; window.expandCall=expandCall;

function setCallAudioUI(nick, avatarData, callType){
  var avEl=document.getElementById('callAudioAvatar');
  var nameEl=document.getElementById('callAudioName'); if(nameEl) nameEl.textContent=nick||'';
  var initEl=document.getElementById('callAudioInitial'); if(initEl) initEl.textContent=(nick||'?')[0].toUpperCase();
  if(avEl){
    if(avatarData) avEl.innerHTML='<img src="'+avatarData+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">';
    else { avEl.innerHTML=''; avEl.textContent=(nick||'?')[0].toUpperCase(); avEl.style.background='var(--accent)'; }
  }
  var st=document.getElementById('callAudioStatus'); if(st) st.textContent='Вызов...';
  var mn=document.getElementById('callMiniName'); if(mn) mn.textContent=nick||'Звонок...';
  var scrn=document.getElementById('callAudioScreen');
  var lv=document.getElementById('localVideo');
  var fcb=document.getElementById('flipCamBtn');
  if(callType==='audio'){
    if(scrn) scrn.classList.add('show');
    if(lv) lv.style.display='none';
    if(fcb) fcb.style.display='none';
  } else {
    if(scrn) scrn.classList.remove('show');
    if(lv) lv.style.display='';
    if(fcb) fcb.style.display='';
  }
}

// ── ICE helpers ───────────────────────────────────────
function flushPendingIce(){
  if(pendingIce.length>0&&peerConnection&&peerConnection.remoteDescription){
    var toAdd=pendingIce.splice(0);
    toAdd.forEach(function(c){ peerConnection.addIceCandidate(new RTCIceCandidate(c)).catch(function(){}); });
  }
}

function createPeerConnection(targetLogin){
  if(peerConnection){ try{peerConnection.close();}catch(e){} }
  peerConnection=new RTCPeerConnection(rtcConfig);
  pendingIce=[];
  peerConnection.onicecandidate=function(e){
    if(e.candidate) socket.emit('iceCandidate',{candidate:e.candidate,to:targetLogin});
  };
  peerConnection.oniceconnectionstatechange=function(){
    var s=peerConnection.iceConnectionState;
    console.log('ICE:',s);
    if(s==='connected'||s==='completed'){
      startCallTimer();
      var st=document.getElementById('callAudioStatus'); if(st) st.textContent='0:00';
    }
    if(s==='failed'){ try{peerConnection.restartIce();}catch(e){endCall();} }
    if(s==='disconnected'){
      setTimeout(function(){
        if(peerConnection&&peerConnection.iceConnectionState==='disconnected') endCall();
      },5000);
    }
  };
  peerConnection.ontrack=function(e){
    var rv=document.getElementById('remoteVideo');
    if(!rv.srcObject) rv.srcObject=new MediaStream();
    rv.srcObject.addTrack(e.track);
    rv.play().catch(function(){});
    if(e.track.kind==='video'&&activeCallType==='video'){
      var scrn=document.getElementById('callAudioScreen'); if(scrn) scrn.classList.remove('show');
    }
  };
  return peerConnection;
}

// ── Исходящий звонок ──────────────────────────────────
function startCall(userToCall, type){
  if(peerConnection){ console.warn('Already in call'); return; }
  callPartnerLogin=userToCall; activeCallType=type||'audio';
  // Найти ник
  callPartnerNick=userToCall;
  if(currentPMProfile&&currentPMProfile.login===userToCall) callPartnerNick=currentPMProfile.nickname;
  document.querySelectorAll('.chat-item').forEach(function(item){
    if(item.dataset&&item.dataset.login===userToCall){
      var nameEl=item.querySelector('.info .name'); if(nameEl) callPartnerNick=nameEl.textContent;
    }
  });
  showCallOverlay();
  setCallAudioUI(callPartnerNick, avatarCache[userToCall]||null, type);

  // getUserMedia сам запрашивает разрешения (и на Android тоже)
  navigator.mediaDevices.getUserMedia({
    audio:true,
    video: type==='video' ? {facingMode:currentFacingMode,width:{ideal:640},height:{ideal:480}} : false
  })
  .then(function(stream){
    localStream=stream;
    var lv=document.getElementById('localVideo'); if(lv) lv.srcObject=stream;
    if(type!=='video'){ var tvb=document.getElementById('toggleVideoBtn'); if(tvb) tvb.classList.remove('active'); }
    else { var tvb2=document.getElementById('toggleVideoBtn'); if(tvb2) tvb2.classList.add('active'); }
    createPeerConnection(userToCall);
    localStream.getTracks().forEach(function(t){ peerConnection.addTrack(t,localStream); });
    return peerConnection.createOffer({offerToReceiveAudio:true,offerToReceiveVideo:true});
  })
  .then(function(o){ return peerConnection.setLocalDescription(o); })
  .then(function(){ socket.emit('callUser',{userToCall:userToCall,signalData:peerConnection.localDescription,callType:type||'audio'}); })
  .catch(function(e){
    console.error('startCall error:',e);
    alert('Нет доступа к микрофону/камере: '+e.message);
    cleanupCall();
  });
}
window.startCall=startCall;

// ── Входящий звонок ───────────────────────────────────
socket.on('incomingCall',function(d){
  if(peerConnection){
    // Уже в звонке — отклонить
    socket.emit('hangUp',{to:d.from,duration:0,rejected:true});
    return;
  }
  incomingCallData=d; earlyCandidates=[];
  activeCallType=d.callType; callPartnerLogin=d.from; callPartnerNick=d.fromNickname||d.from;

  document.getElementById('incomingCallerName').textContent=d.fromNickname||d.from;
  document.getElementById('incomingCallType').textContent=d.callType==='video'?'📹 Видеозвонок':'📞 Аудиозвонок';
  var avEl=document.getElementById('incomingCallAvatar');
  var av=avatarCache[d.from]||null;
  if(av) avEl.innerHTML='<img src="'+av+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">';
  else { avEl.innerHTML=''; avEl.style.fontSize='36px'; avEl.style.background='var(--accent)'; avEl.textContent=(d.fromNickname||d.from)[0].toUpperCase(); }
  document.getElementById('incomingCallModal').classList.add('show');
  startRinging();

  clearInterval(incomingCallCountdown);
  var sec=20;
  var timerEl=document.getElementById('incomingCallTimer');
  timerEl.textContent='Автосброс через '+sec+' с';
  incomingCallCountdown=setInterval(function(){
    sec--;
    if(sec<=0){ clearInterval(incomingCallCountdown); timerEl.textContent=''; }
    else timerEl.textContent='Автосброс через '+sec+' с';
  },1000);
});

// ── Принять звонок ────────────────────────────────────
function acceptCall(){
  if(!incomingCallData){ console.warn('No incomingCallData'); return; }
  clearInterval(incomingCallCountdown);
  document.getElementById('incomingCallTimer').textContent='';
  stopRinging();
  document.getElementById('incomingCallModal').classList.remove('show');
  showCallOverlay();
  setCallAudioUI(callPartnerNick, avatarCache[callPartnerLogin]||null, incomingCallData.callType);

  var useVideo=incomingCallData.callType==='video';
  var savedData=incomingCallData;

  navigator.mediaDevices.getUserMedia({
    audio:true,
    video: useVideo ? {facingMode:currentFacingMode,width:{ideal:640},height:{ideal:480}} : false
  })
  .then(function(stream){
    localStream=stream;
    var lv=document.getElementById('localVideo'); if(lv) lv.srcObject=stream;
    if(!useVideo){ var tvb=document.getElementById('toggleVideoBtn'); if(tvb) tvb.classList.remove('active'); }
    else { var tvb2=document.getElementById('toggleVideoBtn'); if(tvb2) tvb2.classList.add('active'); }
    createPeerConnection(savedData.from);
    // Перенести ранние ICE кандидаты в pendingIce
    pendingIce=earlyCandidates.slice(); earlyCandidates=[];
    localStream.getTracks().forEach(function(t){ peerConnection.addTrack(t,localStream); });
    return peerConnection.setRemoteDescription(new RTCSessionDescription(savedData.signal));
  })
  .then(function(){ flushPendingIce(); return peerConnection.createAnswer(); })
  .then(function(a){ return peerConnection.setLocalDescription(a); })
  .then(function(){
    socket.emit('answerCall',{signal:peerConnection.localDescription,to:savedData.from});
  })
  .catch(function(e){
    console.error('acceptCall error:',e);
    alert('Ошибка при ответе на звонок: '+e.message);
    cleanupCall();
  });
}
window.acceptCall=acceptCall;

// ── Отклонить звонок ──────────────────────────────────
function rejectCall(){
  clearInterval(incomingCallCountdown);
  document.getElementById('incomingCallTimer').textContent='';
  stopRinging();
  if(incomingCallData) socket.emit('hangUp',{to:incomingCallData.from,duration:0,rejected:true});
  document.getElementById('incomingCallModal').classList.remove('show');
  incomingCallData=null;
}
window.rejectCall=rejectCall;

// ── Звонок принят собеседником ────────────────────────
socket.on('callAccepted',function(signal){
  if(!peerConnection) return;
  peerConnection.setRemoteDescription(new RTCSessionDescription(signal))
  .then(function(){ flushPendingIce(); })
  .catch(function(e){ console.error('callAccepted error:',e); });
});

// ── ICE кандидаты ─────────────────────────────────────
socket.on('iceCandidate',function(candidate){
  if(peerConnection&&peerConnection.remoteDescription){
    peerConnection.addIceCandidate(new RTCIceCandidate(candidate)).catch(function(){});
  } else if(peerConnection){
    pendingIce.push(candidate);
  } else {
    earlyCandidates.push(candidate);
  }
});

// ── Звонок завершён (от сервера) ──────────────────────
socket.on('callEnded',function(data){
  clearInterval(incomingCallCountdown);
  var timerEl=document.getElementById('incomingCallTimer'); if(timerEl) timerEl.textContent='';
  stopRinging();
  document.getElementById('incomingCallModal').classList.remove('show');
  var partner=callPartnerLogin||(incomingCallData?incomingCallData.from:null);
  cleanupCall();
  incomingCallData=null;
  if(currentView==='pm'&&currentPrivateLogin&&partner===currentPrivateLogin){
    setTimeout(function(){ _pmToken++; var tk=_pmToken; socket.emit('getPrivateHistory',{login:currentPrivateLogin,token:tk}); },600);
  }
});

// ── Завершить звонок (кнопка) ─────────────────────────
function endCall(){
  var dur=getDuration();
  var partner=callPartnerLogin||(incomingCallData?incomingCallData.from:null);
  cleanupCall();
  if(partner) socket.emit('hangUp',{to:partner,duration:dur});
}
window.endCall=endCall;

function cleanupCall(){
  stopRinging();
  stopCallTimer();
  if(peerConnection){try{peerConnection.close();}catch(e){} peerConnection=null;}
  if(localStream){localStream.getTracks().forEach(function(t){t.stop();}); localStream=null;}
  var rv=document.getElementById('remoteVideo');
  if(rv&&rv.srcObject){try{rv.srcObject.getTracks().forEach(function(t){t.stop()});}catch(e){} rv.srcObject=null;}
  var lv=document.getElementById('localVideo');
  if(lv&&lv.srcObject){try{lv.srcObject.getTracks().forEach(function(t){t.stop()});}catch(e){} lv.srcObject=null;}
  document.getElementById('callOverlay').classList.remove('show');
  document.getElementById('callMiniBar').classList.remove('show');
  document.getElementById('incomingCallModal').classList.remove('show');
  var cas=document.getElementById('callAudioScreen'); if(cas) cas.classList.remove('show');
  if(lv) lv.style.display='';
  var fcb=document.getElementById('flipCamBtn'); if(fcb) fcb.style.display='';
  callStartTime=0;
  pendingIce=[]; earlyCandidates=[];
  incomingCallData=null;
  callPartnerLogin=null; callPartnerNick='';
}

function toggleMic(){
  if(localStream){var t=localStream.getAudioTracks()[0];if(t){t.enabled=!t.enabled;
    var btn=document.getElementById('toggleMicBtn');
    if(btn){btn.textContent=t.enabled?'🎤':'🔇';btn.style.background=t.enabled?'rgba(255,255,255,0.15)':'#c0392b';}
  }}
}
window.toggleMic=toggleMic;
function toggleVideo(){
  if(localStream){var t=localStream.getVideoTracks()[0];if(t){t.enabled=!t.enabled;
    var btn=document.getElementById('toggleVideoBtn');
    if(btn){if(t.enabled)btn.classList.add('active');else btn.classList.remove('active');}
  }}
}
window.toggleVideo=toggleVideo;

async function switchCamera(){
  if(!localStream) return;
  var nextFacing=currentFacingMode==='user'?'environment':'user';
  try{
    var oldVT=localStream.getVideoTracks()[0];
    if(oldVT){localStream.removeTrack(oldVT);oldVT.stop();}
    var ns=await navigator.mediaDevices.getUserMedia({
      audio:false,
      video:{facingMode:{exact:nextFacing},width:{ideal:640},height:{ideal:480}}
    }).catch(function(){
      return navigator.mediaDevices.getUserMedia({audio:false,video:{facingMode:nextFacing,width:{ideal:640},height:{ideal:480}}});
    });
    currentFacingMode=nextFacing;
    var nvt=ns.getVideoTracks()[0];
    if(peerConnection){
      var sender=peerConnection.getSenders().find(function(s){return s.track&&s.track.kind==='video';});
      if(sender) await sender.replaceTrack(nvt);
    }
    localStream.addTrack(nvt);
    var lv=document.getElementById('localVideo');
    lv.srcObject=null; lv.srcObject=localStream; lv.play().catch(function(){});
  }catch(e){ alert('Не удалось переключить камеру: '+e.message); }
}
window.switchCamera=switchCamera;

// ═══════════════════════════════════════════════════════
// GROUP CALL SYSTEM (mesh, like Telegram)
// ═══════════════════════════════════════════════════════
var gcRoomId=null, gcCallType='audio';
var gcLocalStream=null;
// ─── GROUP CALL STATE ────────────────────────────────
// gcPeers[login] = { pc, audioEl, mediaStream, iceBuffer[] }
var gcPeers={};
var gcNicknames={}; // login -> displayName
var gcTimerInterval=null, gcStartTime=0;
var gcMicOn=true, gcVideoOn=false;
var gcFacingMode='user';

function gcGetRtcConfig(){ return rtcConfig; }

// ─── Join / Start ────────────────────────────────────
function joinGroupCall(callType){
  if(!currentRoomId){ alert('Откройте группу сначала'); return; }
  if(gcRoomId){ alert('Вы уже в групповом звонке'); return; }
  if(peerConnection){ alert('Завершите текущий личный звонок'); return; }
  gcCallType = callType || 'audio';
  gcVideoOn  = (callType === 'video');
  gcRoomId   = currentRoomId;

  navigator.mediaDevices.getUserMedia({
    audio: true,
    video: gcVideoOn ? { facingMode: gcFacingMode, width:{ideal:640}, height:{ideal:480} } : false
  })
  .then(function(stream){
    gcLocalStream = stream;
    document.getElementById('groupCallOverlay').classList.add('show');
    document.getElementById('gcRoomName').textContent =
      (currentRoomData && currentRoomData.name) || ('Группа #' + gcRoomId);
    gcRenderMyTile();
    socket.emit('joinGroupCall', { roomId: gcRoomId, callType: gcCallType });
  })
  .catch(function(e){
    gcRoomId = null;
    alert('Нет доступа к микрофону: ' + e.message);
  });
}
window.joinGroupCall = joinGroupCall;

// ─── Server: joined, here are existing participants ──
socket.on('groupCallJoined', function(d){
  gcStartTime = Date.now();
  clearInterval(gcTimerInterval);
  gcTimerInterval = setInterval(function(){
    var sec = Math.floor((Date.now()-gcStartTime)/1000);
    var m = Math.floor(sec/60), s = sec%60;
    document.getElementById('gcDuration').textContent =
      (m<10?'0':'')+m+':'+(s<10?'0':'')+s;
  }, 1000);

  // Show tiles for existing participants immediately (with "Подключение..." state)
  d.participants.forEach(function(login){
    gcEnsureTile(login);
  });
  gcUpdateGridLayout();
  gcUpdateParticipantCount();

  // WE are the newcomer → send offer to each existing participant
  // Small delay to ensure our stream is ready
  setTimeout(function(){
    d.participants.forEach(function(login){
      gcInitPeer(login, true); // true = we are initiator → send offer
    });
  }, 300);
});

// ─── Server: someone else just joined → WE send them offer ──
socket.on('groupCallParticipantJoined', function(d){
  if(!gcRoomId) return;
  var login = d.login;
  if(login === myLogin) return;
  if(d.nickname) gcNicknames[login] = d.nickname;
  // Show tile immediately
  gcEnsureTile(login);
  gcUpdateGridLayout();
  gcUpdateParticipantCount();
  // We are existing participant → send offer to newcomer
  gcInitPeer(login, true);
});

// ─── Receive offer → create peer, send answer ────────
socket.on('groupCallOffer', function(d){
  if(!gcRoomId) return;
  var from = d.from;  // server sends 'from', not 'login'
  if(from === myLogin) return;
  var peer = gcInitPeer(from, false);  // false = not initiator
  peer.pc.setRemoteDescription(new RTCSessionDescription(d.signal))
  .then(function(){ return peer.pc.createAnswer(); })
  .then(function(a){ return peer.pc.setLocalDescription(a); })
  .then(function(){
    socket.emit('groupCallAnswer', { roomId: gcRoomId, to: from, signal: peer.pc.localDescription });
    gcFlushIce(from);
  })
  .catch(function(e){ console.error('[GC] offer error:', e); });
});

// ─── Receive answer → set remote description ────────
socket.on('groupCallAnswer', function(d){
  if(!gcRoomId) return;
  var from = d.from;  // server sends 'from'
  var peer = gcPeers[from];
  if(!peer) return;
  peer.pc.setRemoteDescription(new RTCSessionDescription(d.signal))
  .then(function(){ gcFlushIce(from); })
  .catch(function(e){ console.error('[GC] answer error:', e); });
});

// ─── ICE candidate ───────────────────────────────────
socket.on('groupCallIce', function(d){
  if(!gcRoomId) return;
  var from = d.from;
  var peer = gcPeers[from];
  if(peer && peer.pc.remoteDescription && peer.pc.remoteDescription.type){
    peer.pc.addIceCandidate(new RTCIceCandidate(d.candidate)).catch(function(){});
  } else if(peer){
    peer.iceBuffer.push(d.candidate);
  } else {
    // Peer not yet created — buffer globally
    if(!gcPeers['_pre_'+from]) gcPeers['_pre_'+from] = [];
    gcPeers['_pre_'+from].push(d.candidate);
  }
});

// ─── Participant left ────────────────────────────────
socket.on('groupCallParticipantLeft', function(d){
  gcRemovePeer(d.login);
  gcUpdateGridLayout();
  gcUpdateParticipantCount();
});

// ─── Group call ended by server ──────────────────────
socket.on('groupCallEnded', function(d){
  if(d.roomId === gcRoomId) gcCleanup();
  var bar = document.getElementById('gcJoinBar_'+d.roomId);
  if(bar) bar.remove();
});

// ─── Active call banner (someone else started a call) ─
socket.on('groupCallActive', function(d){
  if(d.roomId === gcRoomId) return;
  gcShowJoinBar(d);
});

function gcShowJoinBar(d){
  var barId = 'gcJoinBar_'+d.roomId;
  if(document.getElementById(barId)) return;
  var bar = document.createElement('div');
  bar.className = 'gc-join-bar'; bar.id = barId;
  var count = (d.participants && d.participants.length) || 0;
  bar.innerHTML =
    '<div class="gc-join-pulse"></div>'+
    '<div class="gc-join-text">Активный звонок'+
    '<div class="gc-join-sub">'+count+' участник'+(count===1?'':'ов')+'</div></div>'+
    '<button class="gc-join-btn" onclick="joinGroupCall(\'audio\')">Войти</button>';
  if(currentView==='room' && currentRoomId===d.roomId){
    var md = document.getElementById('messages');
    if(md) md.parentNode.insertBefore(bar, md);
  }
}

// ─── Core: create/get peer entry ────────────────────
function gcInitPeer(login, isInitiator){
  // If peer already exists as non-initiator role, don't recreate
  if(gcPeers[login] && !isInitiator){
    return gcPeers[login];
  }
  // Close old connection before recreating
  if(gcPeers[login]){
    try{ gcPeers[login].pc.close(); }catch(e){}
    if(gcPeers[login].audioEl){
      gcPeers[login].audioEl.pause();
      gcPeers[login].audioEl.remove();
    }
  }

  var pc = new RTCPeerConnection(gcGetRtcConfig());

  // Each peer gets a dedicated <audio> element for remote audio
  var audioEl = document.createElement('audio');
  audioEl.autoplay = true;
  audioEl.setAttribute('playsinline', '');
  audioEl.style.display = 'none';
  document.body.appendChild(audioEl);

  // Use separate streams for audio and video to avoid timing issues
  var remoteStream = new MediaStream();
  audioEl.srcObject = remoteStream;

  var peer = { pc: pc, audioEl: audioEl, remoteStream: remoteStream, iceBuffer: [] };
  gcPeers[login] = peer;

  // Transfer any pre-buffered ICE candidates
  var pre = gcPeers['_pre_'+login];
  if(pre){ peer.iceBuffer = pre; delete gcPeers['_pre_'+login]; }

  // Add our local tracks to this peer connection
  if(gcLocalStream){
    gcLocalStream.getTracks().forEach(function(t){ pc.addTrack(t, gcLocalStream); });
  }

  pc.onicecandidate = function(e){
    if(e.candidate) socket.emit('groupCallIce', { to: login, candidate: e.candidate });
  };

  pc.ontrack = function(e){
    var track = e.track;
    // Add track to the remote stream
    remoteStream.addTrack(track);

    if(track.kind === 'audio'){
      // Re-attach srcObject to trigger audio playback (some browsers need this)
      audioEl.srcObject = remoteStream;
      audioEl.play().catch(function(err){
        console.log('[GC] audio play blocked, will retry on user gesture:', err);
      });
    }
    if(track.kind === 'video'){
      gcEnsureVideoTile(login, remoteStream);
    }
    // Ensure tile exists (shows avatar for audio-only)
    gcEnsureTile(login);
  };

  pc.oniceconnectionstatechange = function(){
    var s = pc.iceConnectionState;
    console.log('[GC] ICE ['+login+']:', s);
    if(s === 'failed'){
      try{ pc.restartIce(); }catch(er){}
    }
    if(s === 'connected' || s === 'completed'){
      gcMarkTileConnected(login);
      // Retry audio play in case it was blocked
      audioEl.play().catch(function(){});
    }
    if(s === 'disconnected'){
      // Give 4 seconds to reconnect before removing
      setTimeout(function(){
        if(pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed'){
          gcRemovePeer(login);
          gcUpdateGridLayout();
          gcUpdateParticipantCount();
        }
      }, 4000);
    }
  };

  if(isInitiator){
    pc.createOffer({ offerToReceiveAudio: true, offerToReceiveVideo: true })
    .then(function(o){ return pc.setLocalDescription(o); })
    .then(function(){
      socket.emit('groupCallOffer', { roomId: gcRoomId, to: login, signal: pc.localDescription });
    })
    .catch(function(e){ console.error('[GC] createOffer error:', e); });
  }

  return peer;
}

function gcFlushIce(login){
  var peer = gcPeers[login];
  if(!peer) return;
  if(!peer.pc.remoteDescription || !peer.pc.remoteDescription.type) return;
  var buf = peer.iceBuffer.splice(0);
  buf.forEach(function(c){
    peer.pc.addIceCandidate(new RTCIceCandidate(c)).catch(function(){});
  });
}

function gcRemovePeer(login){
  var peer = gcPeers[login];
  if(!peer) return;
  try{ peer.pc.close(); }catch(e){}
  if(peer.audioEl){ peer.audioEl.pause(); peer.audioEl.srcObject = null; peer.audioEl.remove(); }
  delete gcPeers[login];
  var tile = document.getElementById('gc-tile-'+login);
  if(tile) tile.remove();
}

// ─── Tile management ─────────────────────────────────
function gcEnsureTile(login){
  var tileId = 'gc-tile-'+login;
  if(document.getElementById(tileId)) return;
  var tile = document.createElement('div');
  tile.className = 'gc-tile'; tile.id = tileId;

  var av = document.createElement('div'); av.className = 'gc-tile-avatar';
  // Use known nickname or login initial
  var displayName = gcNicknames[login] || login;
  av.textContent = (displayName[0]||'?').toUpperCase();
  if(avatarCache && avatarCache[login]){
    av.innerHTML = '<img src="'+avatarCache[login]+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">';
  }
  tile.appendChild(av);

  var nm = document.createElement('div'); nm.className = 'gc-tile-name';
  nm.textContent = gcNicknames[login] || login;
  tile.appendChild(nm);

  var mic = document.createElement('div'); mic.className = 'gc-tile-mic'; mic.textContent = '🎤';
  tile.appendChild(mic);

  // "Подключение..." label until ICE connects
  var conn = document.createElement('div'); conn.className = 'gc-tile-connecting';
  conn.textContent = 'Подключение...';
  tile.appendChild(conn);

  document.getElementById('gcGrid').appendChild(tile);
}

function gcEnsureVideoTile(login, stream){
  gcEnsureTile(login);
  var tile = document.getElementById('gc-tile-'+login);
  if(!tile) return;
  var vid = tile.querySelector('video');
  if(!vid){
    vid = document.createElement('video');
    vid.autoplay = true; vid.playsInline = true; vid.muted = false;
    vid.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;object-fit:cover;border-radius:12px;z-index:1;';
    tile.insertBefore(vid, tile.firstChild);
  }
  vid.srcObject = stream;
  vid.play().catch(function(){});
}

function gcMarkTileConnected(login){
  var tile = document.getElementById('gc-tile-'+login);
  if(!tile) return;
  tile.classList.add('connected');
  var conn = tile.querySelector('.gc-tile-connecting');
  if(conn) conn.remove();
}

function gcUpdateNickOnTile(login, nick){
  if(nick) gcNicknames[login] = nick;
  var tile = document.getElementById('gc-tile-'+login);
  if(tile){
    var nm = tile.querySelector('.gc-tile-name');
    if(nm) nm.textContent = nick || login;
    var av = tile.querySelector('.gc-tile-avatar');
    if(av && !av.querySelector('img')) av.textContent = (nick||login)[0].toUpperCase();
  }
}

// Render / refresh own tile
function gcRenderMyTile(){
  var grid = document.getElementById('gcGrid');
  if(!grid) return;
  var myTileId = 'gc-tile-'+myLogin;
  var tile = document.getElementById(myTileId);
  if(!tile){
    tile = document.createElement('div');
    tile.className = 'gc-tile me connected'; tile.id = myTileId;
    grid.insertBefore(tile, grid.firstChild);
  }
  tile.innerHTML = '';
  if(gcLocalStream && gcVideoOn){
    var vid = document.createElement('video');
    vid.autoplay=true; vid.playsInline=true; vid.muted=true;
    vid.srcObject = gcLocalStream;
    vid.style.cssText='position:absolute;inset:0;width:100%;height:100%;object-fit:cover;border-radius:12px;z-index:1;';
    tile.appendChild(vid);
  } else {
    var av = document.createElement('div'); av.className='gc-tile-avatar';
    if(myAvatar) av.innerHTML='<img src="'+myAvatar+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">';
    else av.textContent = (myNickname&&myNickname[0]||myLogin[0]||'?').toUpperCase();
    tile.appendChild(av);
  }
  var nm = document.createElement('div'); nm.className='gc-tile-name'; nm.textContent='Вы';
  tile.appendChild(nm);
  var mic = document.createElement('div'); mic.className='gc-tile-mic'; mic.textContent=gcMicOn?'🎤':'🔇';
  tile.appendChild(mic);
  gcUpdateGridLayout();
  gcUpdateParticipantCount();
}

function gcUpdateGridLayout(){
  var grid = document.getElementById('gcGrid');
  if(!grid) return;
  var n = grid.children.length;
  grid.className = 'gc-grid gc-'+(Math.min(n,9)||1);
}

// Show participant count in header
function gcUpdateParticipantCount(){
  var grid = document.getElementById('gcGrid');
  var dur = document.getElementById('gcDuration');
  if(!grid || !dur) return;
  var n = grid.children.length;
  // Only show count if timer hasn't started yet or alongside time
  if(gcStartTime === 0){
    dur.textContent = n + ' участни'+(n===1?'к':'ков');
  }
}

// ─── Controls ────────────────────────────────────────
function gcToggleMic(){
  if(!gcLocalStream) return;
  gcMicOn = !gcMicOn;
  gcLocalStream.getAudioTracks().forEach(function(t){ t.enabled = gcMicOn; });
  document.getElementById('gcMicBtn').textContent = gcMicOn ? '🎤' : '🔇';
  document.getElementById('gcMicBtn').style.background = gcMicOn ? 'rgba(255,255,255,0.15)' : '#c0392b';
  var myTile = document.getElementById('gc-tile-'+myLogin);
  if(myTile){ var mic=myTile.querySelector('.gc-tile-mic'); if(mic) mic.textContent=gcMicOn?'🎤':'🔇'; }
}
window.gcToggleMic = gcToggleMic;

function gcToggleVideo(){
  if(!gcLocalStream) return;
  gcVideoOn = !gcVideoOn;
  gcLocalStream.getVideoTracks().forEach(function(t){ t.enabled = gcVideoOn; });
  document.getElementById('gcVideoBtn').style.background = gcVideoOn ? 'var(--accent)' : 'rgba(255,255,255,0.15)';
  gcRenderMyTile();
}
window.gcToggleVideo = gcToggleVideo;

async function gcSwitchCamera(){
  if(!gcLocalStream) return;
  var next = gcFacingMode==='user' ? 'environment' : 'user';
  try{
    var oldVT = gcLocalStream.getVideoTracks()[0];
    if(oldVT){ gcLocalStream.removeTrack(oldVT); oldVT.stop(); }
    var ns = await navigator.mediaDevices.getUserMedia({
      audio:false, video:{ facingMode:{exact:next}, width:{ideal:640}, height:{ideal:480} }
    }).catch(function(){
      return navigator.mediaDevices.getUserMedia({audio:false, video:{facingMode:next}});
    });
    gcFacingMode = next;
    var nvt = ns.getVideoTracks()[0];
    Object.values(gcPeers).forEach(function(peer){
      if(!peer || !peer.pc || typeof peer.pc.getSenders !== 'function') return;
      var s = peer.pc.getSenders().find(function(s){ return s.track && s.track.kind==='video'; });
      if(s) s.replaceTrack(nvt);
    });
    gcLocalStream.addTrack(nvt);
    gcRenderMyTile();
  }catch(e){ alert('Не удалось переключить камеру: '+e.message); }
}
window.gcSwitchCamera = gcSwitchCamera;

function minimizeGroupCall(){
  document.getElementById('groupCallOverlay').classList.remove('show');
  document.getElementById('callMiniBar').classList.add('show');
  document.getElementById('callMiniName').textContent = (currentRoomData&&currentRoomData.name)||'Групповой звонок';
  document.getElementById('callMiniTime').textContent = '';
}
window.minimizeGroupCall = minimizeGroupCall;

function leaveGroupCall(){
  if(!gcRoomId) return;
  socket.emit('leaveGroupCall', { roomId: gcRoomId });
  gcCleanup();
}
window.leaveGroupCall = leaveGroupCall;

function gcCleanup(){
  clearInterval(gcTimerInterval); gcTimerInterval=null; gcStartTime=0;
  // Close all peers and remove audio elements
  Object.keys(gcPeers).forEach(function(k){
    if(k.startsWith('_pre_')) return;
    var peer = gcPeers[k];
    if(peer && peer.pc){ try{ peer.pc.close(); }catch(e){} }
    if(peer && peer.audioEl){ peer.audioEl.pause(); peer.audioEl.remove(); }
  });
  gcPeers = {};
  if(gcLocalStream){ gcLocalStream.getTracks().forEach(function(t){ t.stop(); }); gcLocalStream=null; }
  document.getElementById('groupCallOverlay').classList.remove('show');
  document.getElementById('callMiniBar').classList.remove('show');
  var grid = document.getElementById('gcGrid');
  if(grid) grid.innerHTML = '';
  gcRoomId=null; gcVideoOn=false; gcMicOn=true;
}

// Override expandCall / mini-end for group calls
var _origExpandCall = window.expandCall;
window.expandCall = function(){
  if(gcRoomId){
    document.getElementById('groupCallOverlay').classList.add('show');
    document.getElementById('callMiniBar').classList.remove('show');
  } else if(_origExpandCall){ _origExpandCall(); }
};
(function(){
  var btn = document.querySelector('.call-mini-end');
  if(btn) btn.onclick = function(e){
    e.stopPropagation();
    if(gcRoomId) leaveGroupCall(); else endCall();
  };
})();

/* ══════════════════════════════════════════════
   👻 GHOST CHAT SYSTEM
   ══════════════════════════════════════════════ */
var _ghostCode = null;       // current ghost room code (6 digits)
var _ghostRoomId = null;     // server ghost room id
var _ghostAnon = true;       // anonymous mode on by default
var _ghostAnonId = null;     // generated anon display name
var _inGhostChat = false;

// Open the Ghost Chat entry screen
function openGhostEntry() {
  document.getElementById('ghostEntryScreen').style.display = 'block';
  ghostShowStep('ghostStep1');
}
window.openGhostEntry = openGhostEntry;

function closeGhostEntry() {
  document.getElementById('ghostEntryScreen').style.display = 'none';
}
window.closeGhostEntry = closeGhostEntry;

function ghostShowStep(stepId) {
  ['ghostStep1','ghostStep2Create','ghostStep2Join'].forEach(function(id){
    document.getElementById(id).style.display = 'none';
  });
  document.getElementById(stepId).style.display = 'flex';
}

function ghostShowJoin() { ghostShowStep('ghostStep2Join'); document.getElementById('ghostCodeInput').value=''; document.getElementById('ghostJoinError').textContent=''; }
window.ghostShowJoin = ghostShowJoin;

function ghostBackToMain() { ghostShowStep('ghostStep1'); }
window.ghostBackToMain = ghostBackToMain;

// Create a Ghost Chat room
function ghostCreateChat() {
  socket.emit('ghostCreate');
  // Show step immediately with loading state
  ghostShowStep('ghostStep2Create');
  document.getElementById('ghostCodeDisplay').textContent = '...';
  document.getElementById('ghostCodeDisplay').style.letterSpacing = '4px';
  document.getElementById('ghostWaitingLabel').style.display = 'block';
  document.getElementById('ghostWaitingLabel').textContent = '⏳ Генерируем код...';
}
window.ghostCreateChat = ghostCreateChat;

function ghostCopyCode() {
  if(!_ghostCode) { showGhostToast('Код ещё не готов, подождите...'); return; }
  var codeToCopy = _ghostCode.replace(/\s/g, ''); // strip spaces
  var copied = false;
  // Try modern clipboard API first
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(codeToCopy).then(function(){
      showGhostToast('✅ Код ' + codeToCopy + ' скопирован!');
    }).catch(function(){
      fallbackCopy();
    });
  } else {
    fallbackCopy();
  }
  function fallbackCopy(){
    var el = document.createElement('textarea');
    el.value = codeToCopy;
    el.style.cssText = 'position:fixed;top:-9999px;left:-9999px;opacity:0;';
    document.body.appendChild(el);
    el.focus(); el.select();
    try { document.execCommand('copy'); showGhostToast('✅ Код ' + codeToCopy + ' скопирован!'); }
    catch(e) { showGhostToast('Код: ' + codeToCopy + ' (скопируйте вручную)'); }
    el.remove();
  }
}
window.ghostCopyCode = ghostCopyCode;

function ghostCancelCreate() {
  if(_ghostCode) socket.emit('ghostCancel', { code: _ghostCode });
  _ghostCode = null;
  ghostShowStep('ghostStep1');
}
window.ghostCancelCreate = ghostCancelCreate;

// Join by entering code
function ghostJoinByCode() {
  var code = (document.getElementById('ghostCodeInput').value||'').trim();
  if(code.length !== 6) { document.getElementById('ghostJoinError').textContent = 'Введите 6-значный код'; return; }
  document.getElementById('ghostJoinError').textContent = '';
  socket.emit('ghostJoin', { code: code });
}
window.ghostJoinByCode = ghostJoinByCode;

// Toggle anon mode
function ghostToggleAnon() {
  _ghostAnon = !_ghostAnon;
  var sw = document.getElementById('ghostAnonToggleSw');
  if(sw) sw.className = 'ghost-toggle-sw' + (_ghostAnon ? ' on' : '');
  if(_ghostRoomId) socket.emit('ghostSetAnon', { roomId: _ghostRoomId, anon: _ghostAnon });
}
window.ghostToggleAnon = ghostToggleAnon;

function openGhostSettings() {
  var sw = document.getElementById('ghostAnonToggleSw');
  if(sw) sw.className = 'ghost-toggle-sw' + (_ghostAnon ? ' on' : '');
  document.getElementById('ghostSettingsModal').style.display = 'flex';
}
window.openGhostSettings = openGhostSettings;

// Leave ghost chat — destroys history for all
function ghostLeaveChat() {
  if(!confirm('Выйти из Ghost Chat? История будет стёрта у ВСЕХ участников.')) return;
  document.getElementById('ghostSettingsModal').style.display = 'none';
  var roomId = _ghostRoomId; // save before cleanup nulls it
  ghostCleanup(); // wipe UI first
  socket.emit('ghostLeave', { roomId: roomId }); // then tell server
}
window.ghostLeaveChat = ghostLeaveChat;

function ghostCleanup() {
  // Wipe messages FIRST before nulling state
  var md = document.getElementById('messages');
  if(md) md.innerHTML = '';

  _ghostCode = null; _ghostRoomId = null; _inGhostChat = false;
  document.getElementById('ghostEntryScreen').style.display = 'none';
  document.getElementById('ghostSettingsModal').style.display = 'none';

  // Remove ghost warn bar
  var wb = document.getElementById('ghostWarnBar');
  if(wb) wb.remove();

  // Go back to main regardless of currentView
  currentView = 'none';
  document.getElementById('mainChat').classList.remove('show');
  document.getElementById('mainChat').classList.remove('ghost-msg-bg');
  document.getElementById('chatTitle').textContent = '';
  document.getElementById('chatHeaderBtns').innerHTML = '';
  document.getElementById('onlineInfo').textContent = '';
  document.getElementById('inputArea').style.display = 'none';
}

// ─── Open Ghost Chat UI ───────────────────────────────────
function openGhostChatUI(roomId, code, partnerAnonId) {
  _inGhostChat = true;
  _ghostRoomId = roomId;
  closeGhostEntry();

  currentView = 'ghost';
  currentPrivateLogin = null; currentRoomId = null; currentRoomData = null;

  document.getElementById('mainChat').classList.add('show');
  document.getElementById('mainChat').classList.add('ghost-msg-bg');
  document.getElementById('noChat').style.display = 'none';
  document.getElementById('inputArea').style.display = 'flex';

  // Header
  document.getElementById('chatTitle').textContent = '👻 Ghost Chat';
  document.getElementById('chatHeaderBtns').innerHTML =
    '<button onclick="openGhostSettings()" title="Настройки" style="color:#a855f7;">⚙️</button>';
  document.getElementById('onlineInfo').innerHTML =
    '<span class="ghost-active-badge">🔒 Ghost #' + code + '</span>';
  document.getElementById('typingIndicator').textContent = '';

  // Warning bar
  var existing = document.getElementById('ghostWarnBar');
  if(existing) existing.remove();
  var warnBar = document.createElement('div');
  warnBar.id = 'ghostWarnBar'; warnBar.className = 'ghost-warn-bar';
  warnBar.textContent = '👻 Ghost Chat — анонимный • история стирается при выходе любого участника';
  var chatBox = document.querySelector('.chat-box') || document.getElementById('messages').parentNode;
  chatBox.insertBefore(warnBar, chatBox.firstChild);

  // Clear messages
  var md = document.getElementById('messages');
  md.innerHTML = '<div style="text-align:center;padding:20px;color:rgba(255,255,255,0.3);font-size:13px;">👻 Ghost Chat начат. Все сообщения исчезнут при выходе любого из участников.</div>';
  md.onscroll = null; // no pagination in ghost chat

  // Disable forwarding and context menus for ghost
  if(window.innerWidth<=640) document.getElementById('mobileBackBtn').style.display='flex';

  // Store partner anon id
  _ghostAnonId = _ghostAnon ? ('Ghost #' + Math.floor(Math.random()*9000+1000)) : null;

  showGhostToast('Ghost Chat открыт! Оба участника подключены.');
}

// ─── Prevent context menu / forwarding in ghost chat ─────
document.addEventListener('contextmenu', function(e){
  if(currentView === 'ghost') { e.preventDefault(); return false; }
});
document.addEventListener('copy', function(e){
  if(currentView === 'ghost') { e.preventDefault(); showGhostToast('Копирование запрещено в Ghost Chat'); }
});

// ─── Send ghost message ──────────────────────────────────
function sendGhostMsg() {
  var inp = document.getElementById('msgInput');
  var text = (inp.value||'').trim();
  if(!text || !_ghostRoomId) return;
  inp.value = '';
  var displayName = _ghostAnon ? _ghostAnonId : (myNickname || myLogin);
  socket.emit('ghostMessage', { roomId: _ghostRoomId, text: text, anon: _ghostAnon });
}

// Ghost message sending is handled directly in sendBtn click handler above

// ─── Socket events ────────────────────────────────────────
// Server confirms room created, sends code
socket.on('ghostCreated', function(d){
  _ghostCode = d.code;
  var display = document.getElementById('ghostCodeDisplay');
  if(display){
    // Show code with spaces between digits for readability
    display.textContent = d.code.split('').join(' ');
    display.style.letterSpacing = '8px';
  }
  var waitLabel = document.getElementById('ghostWaitingLabel');
  if(waitLabel){
    waitLabel.style.display = 'block';
    waitLabel.textContent = '⏳ Ожидаем собеседника...';
  }
});

// Server: someone joined (for creator)
socket.on('ghostReady', function(d){
  _ghostRoomId = d.roomId;
  document.getElementById('ghostWaitingLabel').style.display = 'none';
  // Small delay to show "ready" state then open
  setTimeout(function(){
    openGhostChatUI(d.roomId, _ghostCode, d.partnerAnonId);
  }, 500);
});

// Server: join accepted (for joiner)
socket.on('ghostJoined', function(d){
  _ghostCode = d.code;
  _ghostRoomId = d.roomId;
  openGhostChatUI(d.roomId, d.code, d.partnerAnonId);
});

// Server: join error
socket.on('ghostError', function(d){
  var err = document.getElementById('ghostJoinError');
  if(err) err.textContent = d.message || 'Неверный или истёкший код';
});

// Receive a ghost message
socket.on('ghostMessage', function(d){
  if(currentView !== 'ghost' || !_inGhostChat) return;
  var md = document.getElementById('messages');
  var isMe = (d.fromLogin === myLogin);
  var displayName = d.anon ? d.anonId || 'Ghost' : (isMe ? (myNickname||myLogin) : (d.fromNick||d.fromLogin));

  // Build content HTML based on type
  var contentHtml = '';
  var t = d.type || 'text';
  if(t === 'text' || t === 'sticker'){
    if(t === 'sticker') contentHtml = '<span style="font-size:52px;line-height:1.2;display:block;">'+escHtml(d.text||'')+'</span>';
    else contentHtml = '<div class="msg-text">'+escHtml(d.text||'')+'</div>';
  } else if(t === 'image'){
    var imgSrc = (typeof d.image==='string') ? d.image : (Array.isArray(d.image)?d.image[0]:null);
    if(imgSrc) contentHtml = '<img src="'+imgSrc+'" style="max-width:240px;max-height:240px;border-radius:12px;display:block;cursor:pointer;" onclick="window.pvSingle&&pvSingle(this.src)">';
    else contentHtml = '<div class="msg-text">[фото]</div>';
  } else if(t === 'video'){
    contentHtml = '<video src="'+(d.file_url||'')+'" controls style="max-width:240px;border-radius:12px;display:block;"></video>';
    if(d.file_name) contentHtml += '<div style="font-size:12px;color:rgba(255,255,255,0.5);margin-top:4px;">'+escHtml(d.file_name)+'</div>';
  } else if(t === 'video_note'){
    contentHtml = typeof renderVideoNote==='function'
      ? renderVideoNote(d.voice, isMe, null)
      : '<video src="'+(d.voice||'')+'" controls style="width:160px;height:160px;border-radius:50%;object-fit:cover;display:block;"></video>';
    if(typeof contentHtml === 'object' && contentHtml && contentHtml.outerHTML) contentHtml = contentHtml.outerHTML;
  } else if(t === 'voice'){
    contentHtml = '<audio controls src="'+(d.voice||'')+'" style="max-width:220px;"></audio>';
  } else if(t === 'file'){
    contentHtml = '<a href="'+(d.file_url||'#')+'" download="'+(d.file_name||'file')+'" style="color:#a855f7;text-decoration:none;display:flex;align-items:center;gap:8px;">📎 '+escHtml(d.file_name||'Файл')+'</a>';
  } else {
    contentHtml = '<div class="msg-text">'+escHtml(d.text||'')+'</div>';
  }

  var div = document.createElement('div');
  div.className = 'message ' + (isMe ? 'own' : 'their');
  div.style.cssText = 'user-select:none;-webkit-user-select:none;';

  // For video_note, insert directly
  if(t === 'video_note' && typeof renderVideoNote === 'function'){
    var vnWrap = renderVideoNote(d.voice, isMe, null);
    if(vnWrap && typeof vnWrap === 'object'){
      div.appendChild(vnWrap);
      md.appendChild(div); md.scrollTop = md.scrollHeight; return;
    }
  }

  div.innerHTML =
    '<div class="msg-bubble" oncontextmenu="return false;" ondragstart="return false;">' +
      (!isMe ? '<div class="msg-sender" style="color:#a855f7;">'+escHtml(displayName)+'</div>' : '') +
      contentHtml +
      '<div class="msg-meta"><span class="msg-time">'+formatTime(new Date())+'</span>'+
        (d.anon ? ' <span class="msg-anon-badge">👤 anon</span>' : '')+
      '</div>' +
    '</div>';
  md.appendChild(div);
  md.scrollTop = md.scrollHeight;
});

// Server: partner left — destroy everything
socket.on('ghostEnded', function(d){
  // Wipe messages immediately BEFORE cleanup changes currentView
  var md = document.getElementById('messages');
  if(md) md.innerHTML = '';
  showGhostToast('💨 Ghost Chat завершён. История стёрта.');
  ghostCleanup();
});

// Toast helper
function showGhostToast(msg) {
  var t = document.createElement('div');
  t.style.cssText = 'position:fixed;bottom:90px;left:50%;transform:translateX(-50%);' +
    'background:rgba(124,58,237,0.95);color:#fff;padding:10px 20px;border-radius:20px;' +
    'font-size:13px;font-weight:600;z-index:99999;pointer-events:none;' +
    'box-shadow:0 4px 20px rgba(124,58,237,0.5);letter-spacing:0.3px;';
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(function(){ t.style.opacity='0';t.style.transition='opacity 0.4s'; setTimeout(function(){t.remove();},400); }, 2800);
}
window.showGhostToast = showGhostToast;

// Handle page unload — leave ghost chat
window.addEventListener('beforeunload', function(){
  if(_inGhostChat && _ghostRoomId) {
    var rid = _ghostRoomId;
    socket.emit('ghostLeave', { roomId: rid });
  }
});

/* ── VIDEO NOTES ── */
var vnRecorder=null,vnChunks=[],vnInterval=null,vnSeconds=0,vnStream=null,vnFacingMode='user';
document.getElementById('videoNoteBtn').addEventListener('click',async function(){if(vnRecorder&&vnRecorder.state==='recording')return;vnFacingMode='user';await startVNCamera();});
async function startVNCamera(){
  try{
    if(vnStream)vnStream.getTracks().forEach(function(t){t.stop();});
    vnStream=await navigator.mediaDevices.getUserMedia({video:{facingMode:vnFacingMode,width:{ideal:400},height:{ideal:400}},audio:true});
    document.getElementById('vnPreview').srcObject=vnStream;
    vnChunks=[];var mt=MediaRecorder.isTypeSupported('video/webm;codecs=vp9')?'video/webm;codecs=vp9':'video/webm';
    vnRecorder=new MediaRecorder(vnStream,{mimeType:mt});vnRecorder.ondataavailable=function(e){if(e.data.size>0)vnChunks.push(e.data);};vnRecorder.start();
    document.getElementById('videoNoteModal').classList.add('active');document.getElementById('inputArea').style.display='none';
    vnSeconds=0;document.getElementById('vnTime').textContent='0:00';clearInterval(vnInterval);
    vnInterval=setInterval(function(){vnSeconds++;var m=Math.floor(vnSeconds/60),s=vnSeconds%60;document.getElementById('vnTime').textContent=m+':'+(s<10?'0':'')+s;if(vnSeconds>=60)document.getElementById('vnSend').click();},1000);
  }catch(e){alert('Нет доступа к камере: '+e.message);stopVN();}
}
document.getElementById('vnFlip').addEventListener('click',async function(){
  if(!vnRecorder)return;var sc=vnChunks.slice(),wr=vnRecorder.state==='recording';
  if(wr){vnRecorder.onstop=function(){};vnRecorder.stop();}
  if(vnStream)vnStream.getTracks().forEach(function(t){t.stop();});
  vnFacingMode=vnFacingMode==='user'?'environment':'user';
  try{vnStream=await navigator.mediaDevices.getUserMedia({video:{facingMode:vnFacingMode,width:{ideal:400},height:{ideal:400}},audio:true});document.getElementById('vnPreview').srcObject=vnStream;var mt=MediaRecorder.isTypeSupported('video/webm;codecs=vp9')?'video/webm;codecs=vp9':'video/webm';vnRecorder=new MediaRecorder(vnStream,{mimeType:mt});vnChunks=sc;vnRecorder.ondataavailable=function(e){if(e.data.size>0)vnChunks.push(e.data);};vnRecorder.start();}catch(e){alert('Не удалось переключить камеру');}
});
document.getElementById('vnCancel').addEventListener('click',function(){if(vnRecorder&&vnRecorder.state==='recording'){vnRecorder.onstop=function(){};vnRecorder.stop();}stopVN();});
document.getElementById('vnSend').addEventListener('click',function(){
  if(!vnRecorder)return;
  if(vnRecorder.state==='recording'){
    vnRecorder.onstop=function(){
      if(vnStream)vnStream.getTracks().forEach(function(t){t.stop();});
      var blob=new Blob(vnChunks,{type:'video/webm'});var reader=new FileReader();
      reader.onload=function(e){
        var data=e.target.result;
        if(currentView==='ghost'&&_ghostRoomId){socket.emit('ghostMessage',{roomId:_ghostRoomId,voice:data,type:'video_note',anon:_ghostAnon});}
        else if(currentView==='pm'&&currentPrivateLogin)socket.emit('privateMessage',{toLogin:currentPrivateLogin,voice:data,type:'video_note'});
        else if(currentView==='room'&&currentRoomId)socket.emit('roomMessage',{roomId:currentRoomId,voice:data,type:'video_note'});
        else if(currentView==='general')socket.emit('chatMessage',{voice:data,type:'video_note'});
      };reader.readAsDataURL(blob);};vnRecorder.stop();}stopVN();
});
function stopVN(){clearInterval(vnInterval);if(vnStream){vnStream.getTracks().forEach(function(t){t.stop();});vnStream=null;}vnRecorder=null;vnChunks=[];document.getElementById('videoNoteModal').classList.remove('active');document.getElementById('inputArea').style.display='flex';document.getElementById('vnPreview').srcObject=null;}

function renderVideoNote(src,isMine,msgId){
  var CIRCUMFERENCE=580; // 2*pi*r ≈ 2*3.14159*92.3
  var wrap=document.createElement('div');wrap.className='video-note-wrap';
  if(msgId){
    wrap.dataset.vnId=msgId;
    // Check if already watched
    try{ var w=JSON.parse(localStorage.getItem('vn_watched')||'{}'); if(w[msgId]) wrap.classList.add('watched'); }catch(e){}
  }
  // SVG ring
  var ns='http://www.w3.org/2000/svg';
  var svg=document.createElementNS(ns,'svg');svg.setAttribute('class','vn-ring');svg.setAttribute('viewBox','0 0 200 200');
  var track=document.createElementNS(ns,'circle');track.setAttribute('class','vn-ring-track');track.setAttribute('cx','100');track.setAttribute('cy','100');track.setAttribute('r','97');
  var prog=document.createElementNS(ns,'circle');prog.setAttribute('class','vn-ring-progress');prog.setAttribute('cx','100');prog.setAttribute('cy','100');prog.setAttribute('r','97');
  if(!wrap.classList.contains('watched')) prog.style.strokeDashoffset=CIRCUMFERENCE;
  svg.appendChild(track);svg.appendChild(prog);
  var vid=document.createElement('video');vid.src=src;vid.loop=false;vid.playsInline=true;vid.preload='metadata';
  var btn=document.createElement('button');btn.className='vn-play-btn';btn.textContent='▶';
  var dur=document.createElement('span');dur.className='vn-duration';dur.textContent='0:00';
  vid.addEventListener('loadedmetadata',function(){
    if(isFinite(vid.duration)){var m=Math.floor(vid.duration/60),s=Math.floor(vid.duration%60);dur.textContent=m+':'+(s<10?'0':'')+s;}
  });
  vid.addEventListener('timeupdate',function(){
    if(!vid.paused&&isFinite(vid.duration)&&vid.duration>0){
      var pct=vid.currentTime/vid.duration;
      var offset=CIRCUMFERENCE-(pct*CIRCUMFERENCE);
      prog.style.strokeDashoffset=offset;
      var rem=vid.duration-vid.currentTime,m=Math.floor(rem/60),s=Math.floor(rem%60);
      dur.textContent=m+':'+(s<10?'0':'')+s;
    }
  });
  vid.addEventListener('ended',function(){
    btn.textContent='▶';
    prog.style.strokeDashoffset=0;
    prog.style.stroke='#888';
    wrap.classList.add('watched');
    if(isFinite(vid.duration)){var m=Math.floor(vid.duration/60),s=Math.floor(vid.duration%60);dur.textContent=m+':'+(s<10?'0':'')+s;}
    // Save watched state
    if(msgId){
      try{var w=JSON.parse(localStorage.getItem('vn_watched')||'{}');w[msgId]=1;localStorage.setItem('vn_watched',JSON.stringify(w));}catch(e){}
    }
  });
  btn.onclick=function(){if(vid.paused){vid.play();btn.textContent='⏸';}else{vid.pause();btn.textContent='▶';}};
  wrap.appendChild(svg);wrap.appendChild(vid);wrap.appendChild(btn);wrap.appendChild(dur);
  return wrap;
}

/* ── PINNED CHATS ── */
socket.on('pinnedChats',function(rows){
  pinnedChats=rows;
  renderChatList();
});
socket.on('pinChatResult',function(d){
  if(d.ok){socket.emit('getPinnedChats');}
});
socket.on('unpinChatResult',function(d){
  if(d.ok){socket.emit('getPinnedChats');}
});

function isPinned(chatType, chatId){
  return pinnedChats.some(function(p){ return p.chat_type===chatType&&String(p.chat_id)===String(chatId); });
}

var ctxMenu=null;
function showChatCtxMenu(e, chatType, chatId){
  e.preventDefault(); e.stopPropagation();
  if(ctxMenu){ctxMenu.remove();ctxMenu=null;}
  var pinned=isPinned(chatType,chatId);
  var menu=document.createElement('div');
  menu.className='chat-item-ctx-menu';
  menu.style.left=e.clientX+'px';menu.style.top=e.clientY+'px';
  menu.innerHTML='<div onclick="togglePin(\''+chatType+'\',\''+chatId+'\')">'+(pinned?'📌 Открепить':'📌 Закрепить')+'</div>';
  document.body.appendChild(menu);ctxMenu=menu;
  setTimeout(function(){document.addEventListener('click',function rm(){ctxMenu&&ctxMenu.remove();ctxMenu=null;document.removeEventListener('click',rm);});},10);
}
window.showChatCtxMenu=showChatCtxMenu;

function togglePin(chatType, chatId){
  if(isPinned(chatType,chatId)){socket.emit('unpinChat',{chatType,chatId});}
  else{socket.emit('pinChat',{chatType,chatId});}
  if(ctxMenu){ctxMenu.remove();ctxMenu=null;}
}
window.togglePin=togglePin;

/* ── VIP EMOJI FROM PHOTO ── */
function openVipPhotoEmoji(){
  document.getElementById('vipPhotoInput').click();
}
window.openVipPhotoEmoji=openVipPhotoEmoji;

/* ── PHOTO VIEWER JS ── */
var pvImages=[], pvIndex=0, pvMsgId=null, pvSourceEl=null;

function pvClickHandler(el, e){
  e && e.stopPropagation();
  var imgs = JSON.parse(decodeURIComponent(el.dataset.msgs||'[]'));
  var msgId = el.dataset.msgid||'';
  var idx = parseInt(el.dataset.idx||'0',10);
  openPV(imgs, idx, null, msgId);
}

function pvClickGridHandler(el, pi, e){
  e && e.stopPropagation();
  var grid = el.parentNode;
  var imgs = JSON.parse(decodeURIComponent(grid.dataset.msgs||'[]'));
  var msgId = grid.dataset.msgid||'';
  var idx = parseInt(el.dataset.idx||'0',10);
  openPV(imgs, idx, null, msgId);
}

function openPV(images, idx, e, msgId){
  if(e){e.stopPropagation();}
  pvImages = Array.isArray(images)?images:[images];
  pvIndex = idx||0;
  pvMsgId = msgId||null;
  pvSourceEl = e&&e.target?e.target:null;
  _pvShow();
  document.getElementById('photoViewer').classList.add('active');
  document.addEventListener('keydown', pvKeyHandler);
}

function _pvShow(){
  var img=document.getElementById('pvImg');
  img.src=pvImages[pvIndex];
  document.getElementById('pvTitle').textContent='Фото '+(pvIndex+1)+' из '+pvImages.length;
  document.getElementById('pvPrev').style.display=pvImages.length>1&&pvIndex>0?'flex':'none';
  document.getElementById('pvNext').style.display=pvImages.length>1&&pvIndex<pvImages.length-1?'flex':'none';
}

function closePV(){
  document.getElementById('pvMenu').style.display='none';
  document.getElementById('photoViewer').classList.remove('active');
  document.removeEventListener('keydown',pvKeyHandler);
  pvImages=[];pvIndex=0;pvMsgId=null;
}

function pvNav(dir){
  pvIndex=Math.max(0,Math.min(pvImages.length-1,pvIndex+dir));
  _pvShow();
}

function pvKeyHandler(e){
  if(e.key==='Escape')closePV();
  else if(e.key==='ArrowLeft')pvNav(-1);
  else if(e.key==='ArrowRight')pvNav(1);
}

function pvSave(){
  var a=document.createElement('a');
  a.href=pvImages[pvIndex];
  a.download='photo_'+Date.now()+'.jpg';
  a.click();
}

function pvShowInChat(){
  closePV();
  // scroll to message
  if(pvMsgId){
    var el=document.querySelector('[data-msg-id="'+pvMsgId+'"]');
    if(el){el.scrollIntoView({behavior:'smooth',block:'center'});el.style.transition='background 0.5s';el.style.background='rgba(46,169,223,0.25)';setTimeout(function(){el.style.background='';},1500);}
  }
}

function pvToggleMenu(e){
  if(e){e.stopPropagation();}
  var menu=document.getElementById('pvMenu');
  menu.style.display=menu.style.display==='block'?'none':'block';
}

function pvReply(){
  // save values before close clears them
  var msgId=pvMsgId;
  var imgSrc=pvImages[pvIndex];
  closePV();
  // find message and trigger reply
  var el = msgId ? document.querySelector('[data-msg-id="'+msgId+'"]') : null;
  if(el){
    // extract text or use photo placeholder
    var textEl=el.querySelector('.text');
    var replyText=textEl?textEl.textContent.trim():'📷 Фото';
    var nameEl=el.querySelector('.name');
    var replyUser=nameEl?nameEl.textContent.trim():'';
    setReply(msgId, replyUser||'Фото', replyText||'📷 Фото');
  }
}

function pvShare(){
  var src=pvImages[pvIndex];
  if(navigator.share){
    fetch(src).then(function(r){return r.blob();}).then(function(blob){
      var file=new File([blob],'photo.jpg',{type:'image/jpeg'});
      navigator.share({files:[file]}).catch(function(){});
    });
  } else {
    navigator.clipboard&&navigator.clipboard.writeText(src).then(function(){showError('Ссылка скопирована');});
  }
}

function pvDelete(){
  if(!pvMsgId){closePV();return;}
  if(!confirm('Удалить фото?')){return;}
  // Emit delete for the message
  socket.emit('deleteMessage',{msgId:pvMsgId});
  closePV();
}

// Close menu when clicking outside it
document.getElementById('photoViewer').addEventListener('click',function(e){
  var menu=document.getElementById('pvMenu');
  var menuBtn=document.getElementById('pvMenuBtn');
  if(menu.style.display==='block'&&!menu.contains(e.target)&&e.target!==menuBtn){
    menu.style.display='none';
  }
  // close viewer on backdrop (not on image, menu, nav, top bar)
  if(e.target===this){closePV();}
});

/* ================================══════════
   NEW FEATURES
================================══════════ */

/* ── 1. VERIFICATION ── */
socket.on('userVerified', function(d) {
  verifiedCache[d.login] = (d.verified !== false);
  if (d.login === myLogin) myVerified = (d.verified !== false);
  // Refresh visible messages
  document.querySelectorAll('.msg-nick, .name').forEach(function(el) {
    // lightweight: just trigger re-render by reloading chat list
  });
  renderChatList();
});
socket.on('verifyActivateResult', function(d) {
  var el = document.getElementById('verifyResult');
  if (el) el.textContent = d.ok ? '✅ Аккаунт верифицирован!' : (d.msg || 'Ошибка');
  if (d.ok) { myVerified = true; verifiedCache[myLogin] = true; setTimeout(renderSettingsMain, 1500); }
});

/* ── 2. MUTED CHATS ── */
socket.on('mutedChats', function(rows) {
  mutedChats = rows;
  renderChatList();
});
socket.on('muteChatResult', function(d) {
  if (d.ok) {
    socket.emit('getMutedChats');
  }
});

function isChatMuted(chatType, chatId) {
  var now = Date.now();
  return mutedChats.some(function(m) { return m.chat_type === chatType && String(m.chat_id) === String(chatId) && m.muted_until > now; });
}

function showMuteMenu(e, chatType, chatId) {
  e.preventDefault(); e.stopPropagation();
  if (ctxMenu) { ctxMenu.remove(); ctxMenu = null; }
  var pinned = isPinned(chatType, chatId);
  var muted = isChatMuted(chatType, chatId);
  var menu = document.createElement('div');
  menu.className = 'chat-item-ctx-menu';
  menu.style.left = e.clientX + 'px'; menu.style.top = e.clientY + 'px';
  var pinHtml = '<div onclick="togglePin(\''+chatType+'\',\''+chatId+'\')">'+(pinned?'📌 Открепить':'📌 Закрепить')+'</div>';
  var muteHtml = muted
    ? '<div onclick="unmuteChat(\''+chatType+'\',\''+chatId+'\')">🔔 Включить уведомления</div>'
    : '<div>🔇 Заглушить<div style="margin-left:auto;display:flex;flex-direction:column;gap:4px;padding:4px 0;">'+
      '<span onclick="muteChat(\''+chatType+'\',\''+chatId+'\',1)" style="padding:4px 10px;cursor:pointer;font-size:13px;">1 час</span>'+
      '<span onclick="muteChat(\''+chatType+'\',\''+chatId+'\',8)" style="padding:4px 10px;cursor:pointer;font-size:13px;">8 часов</span>'+
      '<span onclick="muteChat(\''+chatType+'\',\''+chatId+'\',24)" style="padding:4px 10px;cursor:pointer;font-size:13px;">24 часа</span>'+
      '<span onclick="muteChat(\''+chatType+'\',\''+chatId+'\',9999)" style="padding:4px 10px;cursor:pointer;font-size:13px;">Навсегда</span>'+
      '</div></div>';
  var extraHtml = '';
  if (chatType === 'pm') {
    var safeChatId = String(chatId).replace(/'/g, "\\'");
    var safeNickForMenu = (function(){
      var c = latestChats.find(function(x){ return x.login === chatId; });
      return c ? (c.nickname||c.login||'').replace(/'/g, "\\'") : String(chatId);
    })();
    extraHtml += '<div onclick="openPmActionSheet(\''+safeChatId+'\',\''+safeNickForMenu+'\');if(ctxMenu){ctxMenu.remove();ctxMenu=null;}">⋯ Блок / Удалить историю</div>';
  }
  if (chatType === 'room') {
    extraHtml += '<div class="ctx-danger" onclick="leaveRoomFromList('+chatId+');if(ctxMenu){ctxMenu.remove();ctxMenu=null;}">🚪 Покинуть</div>';
  }
  menu.innerHTML = pinHtml + muteHtml + extraHtml;
  document.body.appendChild(menu); ctxMenu = menu;
  setTimeout(function(){document.addEventListener('click',function rm(){ctxMenu&&ctxMenu.remove();ctxMenu=null;document.removeEventListener('click',rm);});},10);
}
window.showMuteMenu = showMuteMenu;

// Override showChatCtxMenu to use showMuteMenu
window.showChatCtxMenu = showMuteMenu;

function muteChat(chatType, chatId, hours) {
  socket.emit('muteChat', { chatType, chatId, hours });
  if (ctxMenu) { ctxMenu.remove(); ctxMenu = null; }
}
window.muteChat = muteChat;

function unmuteChat(chatType, chatId) {
  socket.emit('unmuteChat', { chatType, chatId });
  if (ctxMenu) { ctxMenu.remove(); ctxMenu = null; }
}
window.unmuteChat = unmuteChat;

/* ── 3. EDIT MESSAGE ── */

function startEdit(msgId, msgType, currentText) {
  editingMsgId = msgId; editingMsgType = msgType;
  document.getElementById('editBar').classList.add('active');
  var inp = document.getElementById('msgInput');
  inp.value = currentText || '';
  inp.focus();
  closeReactionPanel();
}
window.startEdit = startEdit;

// (cancelEdit is implemented earlier)

socket.on('messageEdited', function(d) {
  var el = document.getElementById('msg-' + d.msgId) || document.getElementById('pm-' + d.msgId) || document.getElementById('rmsg-' + d.msgId);
  if (!el) return;
  var textEl = el.querySelector('.text');
  if (textEl) {
    textEl.innerHTML = escHtml(d.newText);
    // Add edited tag if not there
    if (!el.querySelector('.edited-tag')) {
      var tag = document.createElement('span'); tag.className = 'edited-tag'; tag.textContent = 'ред.';
      var timeEl = el.querySelector('.msg-time'); if (timeEl) timeEl.insertBefore(tag, timeEl.firstChild); else el.appendChild(tag);
    }
  }
});

/* ── 4. FORWARD MESSAGE ── */
var forwardingMsg = null;

function showForwardModal(msgId, msgType) {
  forwardingMsg = { msgId, msgType };
  var list = document.getElementById('forwardList');
  list.innerHTML = '';

  // Saved messages (self)
  var selfItem = document.createElement('div'); selfItem.className = 'forward-item';
  selfItem.innerHTML = '<div class="avatar" style="width:40px;height:40px;font-size:18px;">📁</div><div><div style="font-size:14px;font-weight:600;">Избранное</div></div>';
  selfItem.onclick = function() { doForward('pm', myLogin); };
  list.appendChild(selfItem);

  // PM chats
  if (window._myChatsCache) {
    window._myChatsCache.forEach(function(u) {
      var item = document.createElement('div'); item.className = 'forward-item';
      item.innerHTML = getAvatarHtml(u.login, u.nickname, 40) + '<div><div style="font-size:14px;font-weight:600;">'+escHtml(u.nickname)+'</div></div>';
      item.onclick = function() { doForward('pm', u.login); };
      list.appendChild(item);
    });
  }
  // Room chats
  if (window._myRoomsCache) {
    window._myRoomsCache.forEach(function(r) {
      var item = document.createElement('div'); item.className = 'forward-item';
      item.innerHTML = '<div class="avatar" style="width:40px;height:40px;font-size:18px;">'+(r.type==='channel'?'📢':'👥')+'</div><div><div style="font-size:14px;font-weight:600;">'+escHtml(r.name)+'</div></div>';
      item.onclick = function() { doForward('room', r.id); };
      list.appendChild(item);
    });
  }
  // General
  var genItem = document.createElement('div'); genItem.className = 'forward-item';
  genItem.innerHTML = '<div class="avatar" style="width:40px;height:40px;font-size:18px;">🌐</div><div><div style="font-size:14px;font-weight:600;">Общий чат</div></div>';
  genItem.onclick = function() { doForward('general', null); };
  list.appendChild(genItem);

  document.getElementById('forwardModal').classList.add('show');
  closeReactionPanel();
}
window.showForwardModal = showForwardModal;

function doForward(toType, toId) {
  if (!forwardingMsg) return;
  socket.emit('forwardMessage', { msgType: forwardingMsg.msgType, msgId: forwardingMsg.msgId, toType, toId: String(toId || '') });
  closeForwardModal();
  showError('✅ Сообщение переслано');
}
window.doForward = doForward;

function closeForwardModal() {
  document.getElementById('forwardModal').classList.remove('show');
  forwardingMsg = null;
}
window.closeForwardModal = closeForwardModal;

socket.on('forwardDone', function(d) { /* handled in doForward */ });

/* Cache chats/rooms for forward modal */
socket.on('myRooms', function(rooms) { window._myRoomsCache = rooms; });

/* ── 5. SAVED MESSAGES ── */
function openSavedMessages() {
  // Close settings if open
  var settingsPanel = document.getElementById('settingsPanel');
  if(settingsPanel && settingsPanel.classList.contains('open')) closeSettings();
  switchToPrivate();
  openPM(myLogin, '📁 Избранное');
}
window.openSavedMessages = openSavedMessages;

/* ── 6. GROUP READ RECEIPTS ── */
var readersPopupTimeout = null;

function showReadersPopup(e, msgId) {
  e.stopPropagation();
  socket.emit('getRoomMsgReaders', { msgId: Number(msgId) });
  socket.once('roomMsgReaders', function(d) {
    if (d.msgId != msgId) return;
    var popup = document.getElementById('readersPopup');
    var listEl = document.getElementById('readersPopupList');
    if (!d.readers.length) { listEl.innerHTML = '<div style="color:var(--text-muted);font-size:13px;">Никто ещё не прочитал</div>'; }
    else listEl.innerHTML = d.readers.map(function(r) { return '<div class="rp-reader">'+escHtml(r.nickname)+'</div>'; }).join('');
    popup.style.display = 'block';
    popup.style.left = Math.min(e.clientX, window.innerWidth - 200) + 'px';
    popup.style.top = (e.clientY - popup.offsetHeight - 8) + 'px';
    if (readersPopupTimeout) clearTimeout(readersPopupTimeout);
    readersPopupTimeout = setTimeout(function() { popup.style.display = 'none'; }, 4000);
  });
}
window.showReadersPopup = showReadersPopup;
document.addEventListener('click', function() { document.getElementById('readersPopup').style.display = 'none'; });

// Mark room messages as read when viewing room
function markRoomMessagesRead() {
  if (currentView !== 'room' || !currentRoomId) return;
  var msgs = document.querySelectorAll('#messages [data-msg-id]');
  var ids = [];
  msgs.forEach(function(el) { if (el.dataset.msgId) ids.push(Number(el.dataset.msgId)); });
  if (ids.length) socket.emit('markRoomRead', { roomId: currentRoomId, msgIds: ids });
}

// Call markRoomMessagesRead when room is opened
socket.on('roomData', function() { setTimeout(markRoomMessagesRead, 500); });

/* ── 7. PER-DIALOG BACKGROUND ── */
var currentDialogBg = null;

function applyDialogBg(bgId, bgData) {
  currentDialogBg = { bgId, bgData };
  var msgEl = document.getElementById('messages');
  if (!msgEl) return;
  if (bgId === 'none' || !bgId) {
    msgEl.style.backgroundImage = '';
    msgEl.style.backgroundColor = '';
    // restore global bg
    if (customBgData) applyBackground('custom', customBgData);
    else applyBackground(currentBgId, null);
  } else if (bgId === 'custom' && bgData) {
    msgEl.style.backgroundImage = 'url(' + bgData + ')';
    msgEl.style.backgroundSize = 'cover';
  } else {
    var bg = CHAT_BGSVAR.find(function(b) { return b.id === bgId; });
    if (bg && bg.value) { msgEl.style.backgroundImage = bg.value; msgEl.style.backgroundSize = 'cover'; }
  }
}

socket.on('dialogBgData', function(d) {
  if ((currentView === 'pm' && String(d.chatId) === String(currentPrivateLogin)) ||
      (currentView === 'room' && String(d.chatId) === String(currentRoomId))) {
    applyDialogBg(d.bgId, d.bgData);
  }
});

function openDialogBgSettings() {
  if (!currentPrivateLogin && !currentRoomId) return;
  var chatType = currentView === 'pm' ? 'pm' : 'room';
  var chatId = currentView === 'pm' ? currentPrivateLogin : currentRoomId;
  var body = document.getElementById('settingsBody');
  var cardsHtml = CHAT_BGSVAR.map(function(bg) {
    var active = (currentDialogBg && currentDialogBg.bgId === bg.id) ? ' active' : '';
    var style = bg.id === 'none'
      ? 'background:var(--bg-base);border:2px dashed var(--border);display:flex;align-items:center;justify-content:center;color:var(--text-muted);font-size:12px;'
      : 'background:' + bg.value + ';background-size:cover;';
    return '<div class="bg-card' + active + '" onclick="setDialogBgChoice(\'' + chatType + '\',\'' + chatId + '\',\'' + bg.id + '\',null)" style="' + style + '">'+
      (bg.id === 'none' ? '<span style="position:absolute;font-size:11px;font-weight:600;">Нет</span>' : '') + '</div>';
  }).join('');
  openSettings();
  var settingsBody = document.getElementById('settingsBody');
  settingsBody.innerHTML = '<div class="settings-header" style="padding:10px 16px;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border);"><button onclick="renderSettingsMain()" style="background:transparent;border:none;color:var(--text-secondary);font-size:20px;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center;">←</button><h3 style="font-size:16px;font-weight:700;">Фон этого диалога</h3></div><div class="bg-grid">' + cardsHtml + '<label class="bg-upload-card" for="dialogBgInput"><span style="font-size:28px;">📷</span><span>Загрузить</span></label><input type="file" id="dialogBgInput" accept="image/*" style="display:none;" onchange="handleDialogBgUpload(this,\'' + chatType + '\',\'' + chatId + '\')"></div>';
}
window.openDialogBgSettings = openDialogBgSettings;

function setDialogBgChoice(chatType, chatId, bgId, bgData) {
  socket.emit('setDialogBg', { chatType, chatId, bgId, bgData });
  applyDialogBg(bgId, bgData);
}
window.setDialogBgChoice = setDialogBgChoice;

function handleDialogBgUpload(input, chatType, chatId) {
  var file = input.files[0]; if (!file) return;
  var reader = new FileReader();
  reader.onload = function(e) {
    var data = e.target.result;
    socket.emit('setDialogBg', { chatType, chatId, bgId: 'custom', bgData: data });
    applyDialogBg('custom', data);
  };
  reader.readAsDataURL(file);
}
window.handleDialogBgUpload = handleDialogBgUpload;

/* ── EXTEND REACTION PANEL with Edit/Forward ── */
var _origShowReactionPanel = showReactionPanel;
window.showReactionPanel = function(msgId, msgType, anchorEl, replyUser, replyText, canDelete) {
  closeReactionPanel();
  var panel = document.createElement('div'); panel.className = 'reaction-panel';

  // Reply
  var rb = document.createElement('button'); rb.className = 'rp-reply'; rb.textContent = '↩ Ответить';
  rb.addEventListener('click', function(e) { e.stopPropagation(); setReply(msgId, replyUser, replyText); closeReactionPanel(); });
  panel.appendChild(rb);

  // Forward
  var fb = document.createElement('button'); fb.className = 'rp-reply'; fb.textContent = '📤 Переслать';
  fb.addEventListener('click', function(e) { e.stopPropagation(); showForwardModal(msgId, msgType); });
  panel.appendChild(fb);

  // Pin message
  var pinBtn = document.createElement('button'); pinBtn.className = 'rp-reply'; pinBtn.textContent = '📌 Закрепить';
  pinBtn.addEventListener('click', function(e) {
    e.stopPropagation(); closeReactionPanel();
    pinMessageInChat(msgId, msgType, replyText, replyUser);
  });
  panel.appendChild(pinBtn);

  // Translate
  var trBtn = document.createElement('button'); trBtn.className = 'rp-reply'; trBtn.textContent = '🌍 Перевести';
  trBtn.addEventListener('click', function(e) {
    e.stopPropagation(); closeReactionPanel();
    translateMessage(msgId, replyText);
  });
  panel.appendChild(trBtn);

  // Edit (only own text messages)
  var msgEl = document.getElementById('msg-' + msgId) || document.getElementById('pm-' + msgId) || document.getElementById('rmsg-' + msgId);
  if (msgEl && msgEl.classList.contains('mine')) {
    var textEl = msgEl.querySelector('.text');
    if (textEl) {
      var eb = document.createElement('button'); eb.className = 'rp-reply'; eb.textContent = '✏️ Редактировать';
      eb.addEventListener('click', function(e) { e.stopPropagation(); startEdit(msgId, msgType, textEl.textContent); });
      panel.appendChild(eb);
    }
  }

  var div1 = document.createElement('div'); div1.className = 'rp-divider'; panel.appendChild(div1);

  // Reactions
  EMOJIS.forEach(function(em) {
    var b = document.createElement('button'); b.textContent = em;
    b.addEventListener('click', function(e) { e.stopPropagation(); socket.emit('addReaction', { msgType, msgId, emoji: em }); closeReactionPanel(); });
    panel.appendChild(b);
  });

  // Delete
  if (canDelete) {
    var div2 = document.createElement('div'); div2.className = 'rp-divider'; panel.appendChild(div2);
    var db = document.createElement('button'); db.className = 'rp-delete'; db.textContent = '🗑 Удалить';
    db.addEventListener('click', function(e) {
      e.stopPropagation(); closeReactionPanel();
      if (msgType === 'general') socket.emit('deleteMessage', msgId);
      else if (msgType === 'pm') socket.emit('deletePrivateMessage', msgId);
      else if (msgType === 'room') socket.emit('deleteRoomMessage', { roomId: currentRoomId, msgId });
    });
    panel.appendChild(db);
  }

  document.body.appendChild(panel); activePanel = panel;
  var rect = anchorEl.getBoundingClientRect();
  var pw = panel.offsetWidth || 380;
  var left = rect.left + (rect.width / 2) - (pw / 2);
  if (left < 8) left = 8; if (left + pw > window.innerWidth - 8) left = window.innerWidth - pw - 8;
  var top = rect.top - panel.offsetHeight - 8; if (top < 8) top = rect.bottom + 8;
  panel.style.left = left + 'px'; panel.style.top = top + 'px';
  setTimeout(function() {
    function outsideClose(e) {
      if(activePanel && !activePanel.contains(e.target)) {
        closeReactionPanel();
        document.removeEventListener('pointerdown', outsideClose);
      }
    }
    document.addEventListener('pointerdown', outsideClose);
    window._panelOutsideClose = outsideClose;
  }, 100);
};

function openVerifySettings() {
  var body = document.getElementById('settingsBody');
  body.innerHTML = '<div class="settings-header" style="padding:10px 16px;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border);"><button onclick="renderSettingsMain()" style="background:transparent;border:none;color:var(--text-secondary);font-size:20px;cursor:pointer;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center;">←</button><h3 style="font-size:16px;font-weight:700;">✓ Верификация</h3></div>'
    + '<div style="padding:24px 20px;">'
    + (myVerified
        ? '<div style="text-align:center;font-size:40px;margin-bottom:12px;">✓</div><div style="text-align:center;color:var(--online);font-weight:700;font-size:16px;">Аккаунт верифицирован!</div>'
        : '<div style="color:var(--text-secondary);font-size:14px;margin-bottom:16px;">Введи промокод верификации от администратора. Галочка ✓ будет видна рядом с твоим именем.</div>'
          + '<input id="verifyCodeInput" placeholder="Промокод верификации (VRF-...)" style="width:100%;padding:12px;border:1.5px solid var(--border);border-radius:12px;background:var(--bg-input);color:var(--text-primary);font-size:14px;box-sizing:border-box;margin-bottom:10px;">'
          + '<div id="verifyResult" style="font-size:13px;color:var(--accent);min-height:20px;margin-bottom:10px;"></div>'
          + '<button onclick="submitVerifyCode()" style="width:100%;padding:13px;border:none;border-radius:12px;background:var(--accent);color:#fff;font-size:15px;font-weight:700;cursor:pointer;">Активировать</button>'
      )
    + '</div>';
}
window.openVerifySettings = openVerifySettings;

function submitVerifyCode() {
  var code = document.getElementById('verifyCodeInput').value.trim();
  if (!code) return;
  socket.emit('activateVerify', { code });
}
window.submitVerifyCode = submitVerifyCode;

/* ── ADD VERIFY TAB TO ADMIN PANEL ── */
var _origLoadTab = loadTab;
window.loadTab = function(tab) {
  if (tab === 'verify') {
    currentAdminTab = tab;
    var c = document.getElementById('adminContent');
    c.innerHTML = '<div style="padding:16px"><h3 style="margin-bottom:14px">✓ Коды верификации</h3>'
      + '<button id="genVerifyBtn" style="padding:9px 16px;border:none;border-radius:10px;background:var(--accent);color:#fff;font-weight:700;cursor:pointer;margin-bottom:16px;">+ Создать код</button>'
      + '<div id="verifyCodesList">Загрузка...</div></div>';
    document.getElementById('genVerifyBtn').addEventListener('click', function() {
      socket.emit('adminGenerateVerifyCode');
    });
    socket.emit('adminGetVerifyCodes');
  } else {
    _origLoadTab(tab);
  }
};

socket.on('adminVerifyCodeCreated', function(d) {
  alert('Код верификации создан:\n' + d.code);
  loadTab('verify');
});
socket.on('adminVerifyCodes', function(rows) {
  var el = document.getElementById('verifyCodesList'); if (!el) return;
  if (!rows.length) { el.innerHTML = '<div style="color:var(--text-muted);font-size:13px">Кодов нет</div>'; return; }
  el.innerHTML = rows.map(function(r) {
    return '<div class="admin-vip-code"><span class="code-val">'+r.code+'</span>'+(r.used?'<span class="code-used">Использован: '+r.used_by+'</span>':'<span class="code-ok">Свободен</span>')+'</div>';
  }).join('');
});

function buildFwdHeader(msg) {
  if (!msg.fwd_from_nick) return '';
  return '<div class="fwd-header">📤 Переслано от: ' + escHtml(msg.fwd_from_nick) + '</div>';
}

var _origBuildMsgTime = buildMsgTime;
window.buildMsgTime = function(ts, msgId, isRoom, isMine) {
  if (!ts) return '';
  var d = new Date(Number(ts));
  var h = d.getHours(), m = d.getMinutes();
  var time = (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m;
  var readerBtn = (isRoom && isMine && msgId)
    ? '<span class="msg-checks check-single" style="cursor:pointer;" onclick="showReadersPopup(event,' + msgId + ')" title="Кто прочитал">✓</span>'
    : '';
  return '<div class="msg-time">' + time + readerBtn + '</div>';
};

// ============================================================
// EMOJI & STICKER PANEL
// ============================================================
var emojiPanelOpen = false;
var currentEmojiTab = 'emoji';

var EMOJI_LIST = [
  '😀','😂','😍','🥰','😊','😎','🤔','😅','😭','😱','🥺','😤','🤣','❤️','🔥','👍','👏','🙏','💪','🎉',
  '✨','🌟','💯','🤩','😏','😒','😔','🤗','😜','🤪','😋','🤤','😴','🤯','🥳','😇','🤠','🤑','😈','👀',
  '💀','👋','🖐','✌️','🤞','👊','🤜','🤙','💅','🤦','🤷','💁','🙋','🙅','🙆','🧐','🥴','😶','😬','🫠',
  '🐶','🐱','🐭','🐹','🐰','🦊','🐻','🐼','🐨','🐯','🦁','🐸','🐧','🐦','🦆','🦅','🦋','🐝','🦄','🐉',
  '🍕','🍔','🌮','🍜','🍣','🍩','🎂','🍎','🍓','🍒','🥑','🥦','🌽','🧁','🍦','🍫','☕','🧃','🥤','🍺',
  '⚽','🏀','🏈','⚾','🎾','🏐','🎮','🎯','♟️','🎲','🎸','🎹','🎨','📸','🎬','🎤','🎧','📚','💻','📱',
  '🚀','✈️','🚗','🚢','🏠','🌍','🌈','⛄','🌊','🌴','🌸','🌺','🍀','⭐','🌙','☀️','⚡','❄️','🌪️','🎃',
  '💎','👑','🏆','🎁','🎀','💌','🔑','🔮','💡','📌','❤️‍🔥','💔','💕','💞','💗','💓','💘','💝','🖤','🤍'
];

var STICKER_PACKS = {
  'Базовые': ['👍','👎','😂','😭','🔥','💯','🤣','😱','🤦','🎉','💪','🙏','❤️','💀','✨','🥺','🤔','😎','🤡','👻'],
  'Животные': ['🐶','🐱','🐸','🦊','🐼','🐨','🦁','🐯','🐻','🦄','🐉','🦋','🦆','🐧','🦅','🦀','🦑','🐙','🦭','🐬'],
  'Еда': ['🍕','🍔','🌮','🍜','🍣','🎂','🍩','🍦','🍫','☕','🥑','🍕','🥤','🍺','🍰','🧁','🍪','🥞','🌯','🥗']
};

function toggleEmojiPanel() {
  var panel = document.getElementById('emojiPanel');
  emojiPanelOpen = !emojiPanelOpen;
  panel.style.display = emojiPanelOpen ? 'block' : 'none';
  if (emojiPanelOpen) {
    showEmojiTab('emoji');
    var inputArea = document.getElementById('inputArea');
    var rect = inputArea.getBoundingClientRect();
    panel.style.bottom = (window.innerHeight - rect.top) + 'px';
  }
}

function showEmojiTab(tab) {
  currentEmojiTab = tab;
  ['emoji','sticker','gif'].forEach(function(t) {
    var el = document.getElementById('tab'+t.charAt(0).toUpperCase()+t.slice(1));
    if (!el) return;
    el.style.color = tab===t ? 'var(--accent)' : 'var(--text-muted)';
    el.style.borderBottomColor = tab===t ? 'var(--accent)' : 'transparent';
  });
  var gifSearch = document.getElementById('gifSearchWrap');
  if (gifSearch) gifSearch.style.display = tab==='gif' ? 'block' : 'none';
  
  var grid = document.getElementById('emojiGrid');
  if (tab === 'emoji') {
    grid.innerHTML = EMOJI_LIST.map(function(e) {
      return '<span onclick="insertEmoji(\'' + e + '\')" style="font-size:24px;cursor:pointer;padding:4px;display:inline-block;border-radius:6px;" onmouseover="this.style.background=\'var(--bg-active)\'" onmouseout="this.style.background=\'transparent\'">' + e + '</span>';
    }).join('');
  } else if (tab === 'gif') {
    grid.innerHTML = '<div style="color:var(--text-muted);font-size:13px;padding:10px;text-align:center;">Введи запрос для поиска GIF 🎬</div>';
    searchGifs('привет');
  } else {
    var html = '';
    Object.keys(STICKER_PACKS).forEach(function(pack) {
      html += '<div style="font-size:11px;color:var(--text-muted);font-weight:600;margin:6px 0 4px;">' + pack + '</div>';
      html += STICKER_PACKS[pack].map(function(s) {
        return '<span onclick="sendSticker(\'' + s + '\')" style="font-size:32px;cursor:pointer;padding:4px;display:inline-block;border-radius:8px;" onmouseover="this.style.background=\'var(--bg-active)\'" onmouseout="this.style.background=\'transparent\'">' + s + '</span>';
      }).join('');
    });
    grid.innerHTML = html;
  }
}

function insertEmoji(emoji) {
  var inp = document.getElementById('msgInput');
  var pos = inp.selectionStart || inp.value.length;
  inp.value = inp.value.slice(0, pos) + emoji + inp.value.slice(pos);
  inp.focus();
  inp.selectionStart = inp.selectionEnd = pos + emoji.length;
  document.getElementById('emojiPanel').style.display = 'none';
  emojiPanelOpen = false;
}

function sendSticker(emoji) {
  if (currentView === 'ghost' && _ghostRoomId) {
    socket.emit('ghostMessage', { roomId: _ghostRoomId, text: emoji, type: 'sticker', anon: _ghostAnon });
  } else if (currentView === 'pm' && currentPrivateLogin) {
    socket.emit('privateMessage', { toLogin: currentPrivateLogin, text: emoji, type: 'sticker' });
  } else if (currentView === 'room' && currentRoomId) {
    socket.emit('roomMessage', { roomId: currentRoomId, text: emoji, type: 'sticker' });
  } else if (currentView === 'general') {
    socket.emit('chatMessage', { text: emoji, type: 'sticker' });
  }
  document.getElementById('emojiPanel').style.display = 'none';
  emojiPanelOpen = false;
}

document.addEventListener('click', function(e) {
  var panel = document.getElementById('emojiPanel');
  var btn = document.getElementById('emojiPanelBtn');
  if (emojiPanelOpen && !panel.contains(e.target) && e.target !== btn) {
    panel.style.display = 'none';
    emojiPanelOpen = false;
  }
});

document.getElementById('emojiPanelBtn').addEventListener('click', function(e) {
  e.stopPropagation();
  toggleEmojiPanel();
});

// ============================================================
// FILE/VIDEO UPLOAD
// ============================================================
document.getElementById('fileInput').addEventListener('change', function() {
  var files = Array.from(this.files);
  if (!files.length) return;
  var self = this;
  files.forEach(function(file) {
    if(file.size > 50*1024*1024){alert('Файл слишком большой (макс 50MB): '+file.name);return;}
    var reader = new FileReader();
    reader.onload = function(ev) {
      var isVideo = file.type.startsWith('video/');
      var msgType = isVideo ? 'video' : 'file';
      var payload = { type: msgType, text: file.name, file_url: ev.target.result, file_name: file.name, file_size: file.size };
      if (currentView === 'ghost' && _ghostRoomId) socket.emit('ghostMessage', Object.assign({roomId:_ghostRoomId, anon:_ghostAnon}, payload));
      else if (currentView === 'pm' && currentPrivateLogin) socket.emit('privateMessage', Object.assign({toLogin: currentPrivateLogin}, payload));
      else if (currentView === 'room' && currentRoomId) socket.emit('roomMessage', Object.assign({roomId: currentRoomId}, payload));
      else if (currentView === 'general') socket.emit('chatMessage', payload);
    };
    reader.readAsDataURL(file);
  });
  self.value = '';
});

(function() {
  var origPhotoHandler = document.getElementById('photoInput').onchange;
  document.getElementById('photoInput').addEventListener('change', function() {
    var files = Array.from(this.files);
    var videos = files.filter(function(f) { return f.type.startsWith('video/'); });
    var images = files.filter(function(f) { return f.type.startsWith('image/'); });
    
    videos.forEach(function(file) {
      var reader = new FileReader();
      reader.onload = function(ev) {
        var payload = { type: 'video', text: file.name, file_url: ev.target.result, file_name: file.name, file_size: file.size };
        if (currentView === 'pm' && currentPrivateLogin) { socket.emit('privateMessage', Object.assign({toLogin:currentPrivateLogin}, payload)); }
        else if (currentView === 'room' && currentRoomId) { socket.emit('roomMessage', Object.assign({roomId:currentRoomId}, payload)); }
        else if (currentView === 'general') { socket.emit('chatMessage', payload); }
      };
      reader.readAsDataURL(file);
    });
  }, true); 
})();

(function() {
  var _buildContent = window.buildMsgContent;
  function buildFileHtml(msg) {
    if (msg.type === 'video' && (msg.file_url || msg.image)) {
      return '<video src="' + (msg.file_url || msg.image) + '" controls style="max-width:280px;max-height:220px;border-radius:10px;display:block;margin-top:4px;"></video>' + (msg.file_name ? '<div style="font-size:11px;color:var(--text-muted);margin-top:2px;">📹 ' + msg.file_name + '</div>' : '');
    }
    if (msg.type === 'file' && msg.file_url) {
      var size = msg.file_size ? ' (' + Math.round(msg.file_size/1024) + ' KB)' : '';
      return '<a href="' + msg.file_url + '" download="' + (msg.file_name||'file') + '" style="display:flex;align-items:center;gap:8px;background:var(--bg-input);padding:10px 14px;border-radius:10px;color:var(--text-primary);text-decoration:none;margin-top:4px;max-width:280px;"><span style="font-size:24px;">📄</span><div><div style="font-size:13px;font-weight:600;">' + (msg.file_name||'Файл') + '</div><div style="font-size:11px;color:var(--text-muted);">' + size + '</div></div></a>';
    }
    if (msg.type === 'sticker') {
      return '<span style="font-size:52px;line-height:1.2;display:block;">' + (msg.text||'') + '</span>';
    }
    return null;
  }
  window._buildFileHtml = buildFileHtml;
})();

// ============================================================
// STORIES
// ============================================================
var allStories = [];
var currentStoryIdx = 0;
var storyTimer = null;
var storyFileData = null;
var storyFileType = 'image';
var storyFileLoading = false;

function loadStories() {
  socket.emit('getStories');
}

socket.on('storiesData', function(stories) {
  allStories = stories;
  renderStoriesBar(stories);
});

socket.on('newStory', function(story) {
  var existing = allStories.findIndex(function(s){ return s.id === story.id; });
  if (existing === -1) {
    allStories.unshift(story);
    renderStoriesBar(allStories);
  }
});

socket.on('storyDeleted', function(d) {
  allStories = allStories.filter(function(s){ return s.id !== d.storyId; });
  renderStoriesBar(allStories);
});

socket.on('storyResult', function(d) {
  var btns = document.querySelectorAll('#addStoryModal button');
  var pubBtn = btns[btns.length - 1];
  if(pubBtn) { pubBtn.textContent = 'Опубликовать'; pubBtn.disabled = false; }

  if (d.ok) { closeAddStory(); loadStories(); }
  else alert('Ошибка: ' + (d.msg||'неизвестно'));
});

function renderStoriesBar(stories) {
  var row = document.getElementById('storiesRow');
  if (!row) return;
  var addBtn = row.firstElementChild;
  row.innerHTML = '';
  row.appendChild(addBtn);
  
  var byUser = {};
  stories.forEach(function(s) {
    if (!byUser[s.user_login]) byUser[s.user_login] = [];
    byUser[s.user_login].push(s);
  });
  
  Object.keys(byUser).forEach(function(login) {
    var userStories = byUser[login];
    var first = userStories[0];
    var allViewed = userStories.every(function(s){ return parseInt(s.viewed) > 0; });
    var nick = first.user_nickname || login;
    var initials = nick[0].toUpperCase();
    var isMe = (login === myLogin);
    
    var div = document.createElement('div');
    div.className = 'story-item';
    div.innerHTML = '<div class="story-ring' + (allViewed ? ' viewed' : '') + '"><div class="story-ring-inner">' + initials + '</div></div>' +
      '<span class="story-label">' + (isMe ? 'Я' : nick) + '</span>';
    
    var idx = allStories.findIndex(function(s){ return s.user_login === login; });
    div.onclick = function() { openStoryViewer(idx); };
    if (isMe && userStories.length > 0) {
      div.addEventListener('contextmenu', function(e) {
        e.preventDefault();
        if (confirm('Удалить историю?')) {
          socket.emit('deleteStory', { storyId: userStories[0].id });
        }
      });
    }
    row.appendChild(div);
  });
}

function openAddStory() {
  storyFileData = null;
  storyFileType = 'image';
  storyFileLoading = false;
  var inp = document.getElementById('storyFileInput');
  if (inp) inp.value = '';
  var zone = document.getElementById('storyPickZone');
  if (zone) zone.style.borderColor = '';
  var prev = document.getElementById('storyPreview');
  if (prev) prev.innerHTML = '\uD83D\uDCF7<br><span style="font-size:13px;">Нажми чтобы выбрать фото или видео</span>';
  document.getElementById('storyTextInput').value = '';
  // Reset publish button
  var btns = document.querySelectorAll('#addStoryModal button');
  var pubBtn = btns[btns.length - 1];
  if (pubBtn) { pubBtn.textContent = 'Опубликовать'; pubBtn.disabled = false; }
  document.getElementById('addStoryModal').style.display = 'flex';
}

function closeAddStory() {
  document.getElementById('addStoryModal').style.display = 'none';
  storyFileData = null;
  storyFileLoading = false;
}

// storyFileInput listener is attached below after modal HTML

function submitStory() {
  if (storyFileLoading) { alert('Файл ещё загружается, подожди...'); return; }
  if (!storyFileData) { alert('Выбери фото или видео'); return; }
  var text = document.getElementById('storyTextInput').value.trim();
  var btns = document.querySelectorAll('#addStoryModal button');
  var pubBtn = btns[btns.length - 1];
  if(pubBtn) { pubBtn.textContent = 'Отправка...'; pubBtn.disabled = true; }
  socket.emit('addStory', { mediaUrl: storyFileData, mediaType: storyFileType, text: text || null });
}

function openStoryViewer(idx) {
  currentStoryIdx = idx;
  showStory(idx);
  document.getElementById('storyViewerModal').style.display = 'block';
}

function showStory(idx) {
  if (idx < 0 || idx >= allStories.length) { closeStoryViewer(); return; }
  currentStoryIdx = idx;
  var s = allStories[idx];
  
  socket.emit('viewStory', { storyId: s.id });
  
  var content = document.getElementById('storyViewerContent');
  if (s.media_type === 'video') {
    content.innerHTML = '<video src="' + s.media_url + '" autoplay controls style="max-width:100vw;max-height:80vh;border-radius:8px;"></video>';
  } else {
    content.innerHTML = '<img src="' + s.media_url + '" style="max-width:100vw;max-height:80vh;object-fit:contain;border-radius:8px;">';
  }
  
  var ts = new Date(Number(s.timestamp));
  document.getElementById('storyViewerMeta').textContent = (s.user_nickname||s.user_login) + ' • ' + ts.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
  document.getElementById('storyViewerText').textContent = s.text || '';
  
  clearTimeout(storyTimer);
  var prog = document.getElementById('storyProgress');
  prog.style.transition = 'none';
  prog.style.width = '0%';
  setTimeout(function() {
    prog.style.transition = 'width 7s linear';
    prog.style.width = '100%';
  }, 50);
  storyTimer = setTimeout(function() { nextStory(); }, 7000);
}

function nextStory() {
  clearTimeout(storyTimer);
  showStory(currentStoryIdx + 1);
}

function prevStory() {
  clearTimeout(storyTimer);
  showStory(currentStoryIdx - 1);
}

function closeStoryViewer() {
  clearTimeout(storyTimer);
  document.getElementById('storyViewerModal').style.display = 'none';
  document.getElementById('storyViewerContent').innerHTML = '';
}

(function() {
  function patchMsgText(msg, htmlText) {
    if (msg.type === 'sticker') {
      return '<span style="font-size:52px;line-height:1.2;display:block;">' + (msg.text||'') + '</span>';
    }
    if (msg.type === 'video' && (msg.file_url || msg.image)) {
      var html = htmlText;
      html += '<video src="' + (msg.file_url || msg.image) + '" controls style="max-width:260px;max-height:200px;border-radius:10px;display:block;margin-top:4px;"></video>';
      if (msg.file_name) html += '<div style="font-size:11px;color:var(--text-muted);margin-top:2px;">📹 ' + msg.file_name + '</div>';
      return html;
    }
    if (msg.type === 'file' && msg.file_url) {
      var sz = msg.file_size ? ' (' + Math.round(msg.file_size/1024) + ' KB)' : '';
      return htmlText + '<a href="' + msg.file_url + '" download="' + (msg.file_name||'file') + '" style="display:flex;align-items:center;gap:8px;background:var(--bg-input);padding:10px 14px;border-radius:10px;color:var(--text-primary);text-decoration:none;margin-top:4px;max-width:260px;"><span style="font-size:24px;">📄</span><div><div style="font-size:13px;font-weight:600;">' + (msg.file_name||'Файл') + '</div><div style="font-size:11px;color:var(--text-muted);">' + sz + '</div></div></a>';
    }
    return null; 
  }
  window._patchMsgText = patchMsgText;
})();

var _origAuthSuccess = socket.listeners ? null : null;
var _authSuccessOnce = false;
socket.on('authSuccess', function() {
  if (!_authSuccessOnce) { 
    _authSuccessOnce = true;
    setTimeout(loadStories, 1000);
  }
});




// storyFileInput listener — must be after modal HTML in DOM
(function() {
  var inp = document.getElementById('storyFileInput');
  if (!inp) return; // safety guard
  inp.addEventListener('change', function() {
  var file = this.files[0];
  if (!file) return;
  if (file.size > 30 * 1024 * 1024) { alert('Файл слишком большой. Максимум 30 МБ.'); this.value = ''; return; }
  storyFileType = file.type.startsWith('video/') ? 'video' : 'image';
  var zone = document.getElementById('storyPickZone');
  if (zone) zone.style.borderColor = 'var(--accent)';
  var prev = document.getElementById('storyPreview');
  if (prev) prev.innerHTML = '&#9203;<br><span style="font-size:13px;">Загрузка файла...</span>';
  storyFileLoading = true;
  storyFileData = null;
  var btns = document.querySelectorAll('#addStoryModal button');
  var pubBtn = btns[btns.length - 1];
  if (pubBtn) { pubBtn.disabled = true; pubBtn.textContent = 'Загрузка...'; }
  var fileRef = file;
  this.value = '';
  var reader = new FileReader();
  reader.onload = function(ev) {
    storyFileData = ev.target.result;
    storyFileLoading = false;
    if (pubBtn) { pubBtn.disabled = false; pubBtn.textContent = 'Опубликовать'; }
    if (storyFileType === 'video') {
      prev.innerHTML = '<video src="' + storyFileData + '" style="max-width:100%;max-height:130px;border-radius:10px;display:block;" controls></video><div style="font-size:12px;color:var(--accent);margin-top:6px;font-weight:600;">&#10003; Видео выбрано</div>';
    } else {
      prev.innerHTML = '<img src="' + storyFileData + '" style="max-width:100%;max-height:130px;border-radius:10px;object-fit:contain;display:block;"><div style="font-size:12px;color:var(--accent);margin-top:6px;font-weight:600;">&#10003; Фото выбрано</div>';
    }
  };
  reader.onerror = function() {
    storyFileLoading = false;
    storyFileData = null;
    if (pubBtn) { pubBtn.disabled = false; pubBtn.textContent = 'Опубликовать'; }
    if (prev) prev.innerHTML = '&#10060;<br><span style="font-size:13px;">Ошибка чтения файла</span>';
  };
  reader.readAsDataURL(fileRef);
  });
})();

/* ══════════════════════════════════════════
   BLOCK USER + DELETE CHAT HISTORY
══════════════════════════════════════════ */
var _pmActionLogin = null;
var _pmActionNick = '';
var _pmBlockedStatus = false;

function openPmActionSheet(login, nick) {
  _pmActionLogin = login;
  _pmActionNick = nick || login;
  var title = document.getElementById('pmActionSheetTitle');
  if (title) title.textContent = 'Чат с ' + _pmActionNick;
  // Check block status
  socket.emit('checkBlocked', { login: login });
  document.getElementById('pmActionSheet').classList.add('show');
}
window.openPmActionSheet = openPmActionSheet;

function closePmActionSheet() {
  document.getElementById('pmActionSheet').classList.remove('show');
  _pmActionLogin = null;
}
window.closePmActionSheet = closePmActionSheet;

socket.on('blockStatus', function(d) {
  _pmBlockedStatus = d.iBlockedThem;
  var btn = document.getElementById('pmActionBlockBtn');
  if (btn) {
    if (d.iBlockedThem) {
      btn.textContent = '✅ Разблокировать';
      btn.classList.remove('danger');
    } else {
      btn.textContent = '🚫 Заблокировать';
      btn.classList.add('danger');
    }
  }
});

function pmActionBlock() {
  if (!_pmActionLogin) return;
  if (_pmBlockedStatus) {
    if (confirm('Разблокировать ' + _pmActionNick + '?')) {
      socket.emit('unblockUser', { login: _pmActionLogin });
    }
  } else {
    if (confirm('Заблокировать ' + _pmActionNick + '? Они не смогут писать тебе.')) {
      socket.emit('blockUser', { login: _pmActionLogin });
    }
  }
  closePmActionSheet();
}
window.pmActionBlock = pmActionBlock;

function pmActionDeleteMine() {
  if (!_pmActionLogin) return;
  if (!confirm('Удалить всю историю переписки с ' + _pmActionNick + ' только у себя?')) return;
  socket.emit('deleteChatHistory', { login: _pmActionLogin, forBoth: false });
  closePmActionSheet();
}
window.pmActionDeleteMine = pmActionDeleteMine;

function pmActionDeleteBoth() {
  if (!_pmActionLogin) return;
  if (!confirm('Удалить всю историю переписки с ' + _pmActionNick + ' у ОБОИХ? Это необратимо!')) return;
  socket.emit('deleteChatHistory', { login: _pmActionLogin, forBoth: true });
  closePmActionSheet();
}
window.pmActionDeleteBoth = pmActionDeleteBoth;

socket.on('blockResult', function(d) {
  if (!d.ok) { showError('Ошибка: ' + (d.msg || '')); return; }
  if (d.action === 'blocked') showError('🚫 ' + d.login + ' заблокирован');
  else showError('✅ ' + d.login + ' разблокирован');
});

socket.on('chatHistoryDeleted', function(d) {
  // If currently viewing this chat — clear messages
  if (currentView === 'pm' && currentPrivateLogin === d.login) {
    document.getElementById('messages').innerHTML = '<div class="empty-list">История удалена</div>';
  }
  // Refresh chat list
  emitGetMyChats();
  showError('🗑 История удалена');
});

/* ── Add 3-dot menu button to PM header ── */
function updatePmHeaderBtns(login, nick) {
  var safeLogin = (login||'').replace(/'/g,"\'");
  var safeNick = (nick||'').replace(/'/g,"\'");
  document.getElementById('chatHeaderBtns').innerHTML =
    '<button onclick="startCall(\''+safeLogin+'\',\'video\')" title="Видео">📹</button>' +
    '<button onclick="startCall(\''+safeLogin+'\',\'audio\')" title="Аудио">📞</button>' +
    '<button onclick="openGhostEntry()" title="Ghost Chat" style="color:#a855f7;">👻</button>' +
    '<button onclick="openPmActionSheet(\''+safeLogin+'\',\''+safeNick+'\')" title="Ещё" style="font-size:20px;font-weight:700;letter-spacing:1px;">⋯</button>';
}
window.updatePmHeaderBtns = updatePmHeaderBtns;

/* ── Add leave button to room context menu ── */
function leaveRoomFromList(roomId) {
  if (confirm('Покинуть группу/канал?')) {
    socket.emit('leaveRoom', Number(roomId));
  }
}
window.leaveRoomFromList = leaveRoomFromList;



// ═══════════════════════════════════════════════════════════════════════
// FEATURES BLOCK v7 — fixes pins, mentions, nick click, light theme
// ═══════════════════════════════════════════════════════════════════════

// ── Toast ───────────────────────────────────────────────────────────────
window.showToast = function(msg, ms) {
  var e = document.getElementById('chatError');
  if (!e) return;
  e.textContent = msg;
  e.classList.add('show');
  clearTimeout(e._tt);
  e._tt = setTimeout(function(){ e.classList.remove('show'); }, ms || 2800);
};

// ── AI кнопка (скрыть/показать из настроек) ────────────────────────────
(function(){
  var show = localStorage.getItem('aiChatVisible') === 'true';
  var btn = document.getElementById('aiChatBtn');
  if (btn) btn.style.display = show ? 'flex' : 'none';
})();
window._toggleAiBtn = function() {
  var next = localStorage.getItem('aiChatVisible') !== 'true';
  localStorage.setItem('aiChatVisible', next);
  var btn = document.getElementById('aiChatBtn');
  if (btn) btn.style.display = next ? 'flex' : 'none';
  var desc = document.getElementById('aiSettingsDesc');
  var tgl  = document.getElementById('aiSettingsToggle');
  if (desc) desc.textContent = next ? 'Кнопка показана' : 'Кнопка скрыта';
  if (tgl) { tgl.style.background = next ? 'var(--accent)' : 'rgba(255,255,255,0.15)'; var d=tgl.querySelector('div'); if(d) d.style.left = next?'22px':'2px'; }
  showToast(next ? '🤖 AI кнопка показана' : '🤖 AI кнопка скрыта');
};

// ── Helpers ─────────────────────────────────────────────────────────────
function _chatCtx() {
  if (currentView === 'general') return { chatType: 'general', chatId: 'general' };
  if (currentView === 'room')    return { chatType: 'room',    chatId: String(currentRoomId) };
  if (currentView === 'pm')      return { chatType: 'pm',      chatId: String(currentPrivateLogin) };
  return null;
}
function _scrollToMsg(id) {
  var el = document.getElementById('msg-' + id)   ||
           document.getElementById('pm-'  + id)   ||
           document.getElementById('rmsg-'+ id);
  if (!el) return;
  el.scrollIntoView({ behavior: 'smooth', block: 'center' });
  var orig = el.style.background;
  el.style.transition = 'background 0.4s';
  el.style.background = 'rgba(92,160,220,0.35)';
  setTimeout(function(){ el.style.background = orig || ''; }, 1800);
}

// ══════════════════════════════════════════════════════════════════════════
// 0. КЛИК НА НИК → открыть профиль пользователя
// ══════════════════════════════════════════════════════════════════════════
window.onMsgNickClick = function(el) {
  // Prevent triggering reaction panel
  event && event.stopPropagation && event.stopPropagation();
  var login = el.getAttribute('data-login');
  if (!login) return;
  var nick = el.textContent.trim().replace(/^\s*[^\s]+\s+/, '').trim() || login;
  // Show modal immediately with cached data, then refresh from server
  showProfileModal(login, {
    login:    login,
    nickname: nick,
    username: null,
    avatar:   avatarCache[login] || null
  });
  socket.emit('getProfile', { login: login });
};

// Patch userProfile event to update open modal
(function(){
  var origHandler = null;
  socket.on('userProfile', function(p) {
    if (p.avatar) avatarCache[p.login] = p.avatar;
    if (p.verified) verifiedCache[p.login] = true;
    if (p.vip_emoji && p.vip_until > Date.now()) vipEmojiCache[p.login] = p.vip_emoji;
    // Update open profile modal if it's showing this user
    var overlay = document.getElementById('profileModalOverlay');
    if (overlay && overlay.classList.contains('show')) {
      var loginEl = document.getElementById('profileModalLogin');
      if (loginEl && loginEl.textContent === 'логин: ' + p.login) {
        showProfileModal(p.login, p);
      }
    }
  });
})();


// ══════════════════════════════════════════════════════════════════════════
// 1. ЗАКРЕП СООБЩЕНИЙ 📌
// ══════════════════════════════════════════════════════════════════════════
var _pinMsgId = null;

window._onPinBarClick = function(e) {
  if (e && e.target && e.target.classList.contains('pm-close')) return;
  if (_pinMsgId) _scrollToMsg(_pinMsgId);
};
window._onPinClose = function(e) {
  if (e) { e.stopPropagation(); e.preventDefault(); }
  var ctx = _chatCtx();
  if (ctx) socket.emit('unpinMessage', ctx);
};

function _showPinBar(msgId, msgText) {
  _pinMsgId = msgId;
  var bar = document.getElementById('pinnedMsgBar');
  var txt = document.getElementById('pinnedMsgText');
  if (!bar) return;
  if (txt) txt.textContent = (msgText || 'Сообщение').slice(0, 90);
  bar.classList.add('show');
  bar.style.display = 'flex'; // inline backup
}
function _hidePinBar() {
  _pinMsgId = null;
  var bar = document.getElementById('pinnedMsgBar');
  if (bar) { bar.classList.remove('show'); bar.style.display = ''; }
}
function _loadPin() {
  var ctx = _chatCtx();
  if (!ctx) return;
  socket.emit('getPinnedMessage', ctx);
}

// pinMessageInChat вызывается из reaction panel
// msgId — число, msgType — строка типа чата, msgText — текст, msgUser — ник автора
function pinMessageInChat(msgId, msgType, msgText, msgUser) {
  var ctx = _chatCtx();
  // Fallback: if ctx is null, reconstruct from msgType + globals
  if (!ctx) {
    if (msgType === 'general') ctx = { chatType: 'general', chatId: 'general' };
    else if (msgType === 'pm' && currentPrivateLogin) ctx = { chatType: 'pm', chatId: String(currentPrivateLogin) };
    else if (msgType === 'room' && currentRoomId) ctx = { chatType: 'room', chatId: String(currentRoomId) };
  }
  if (!ctx) { showToast('⚠️ Открой чат'); return; }
  socket.emit('pinMessage', {
    chatType: ctx.chatType,
    chatId:   ctx.chatId,
    msgId:    Number(msgId),
    msgText:  (msgText || '').slice(0, 200),
    msgUser:  msgUser || ''
  });
  // Показываем бар сразу (optimistic UI), сервер подтвердит через messagePinned
  _showPinBar(Number(msgId), msgText || '');
}
window.pinMessageInChat = pinMessageInChat;

// Helper: normalize PM chatId for comparison
function _normPmChatId(chatId) {
  // Server stores PM pins as "loginA:loginB" (sorted)
  // Client ctx.chatId is currentPrivateLogin
  if (!myLogin || !chatId) return String(chatId || '');
  var pair = [myLogin, String(chatId)].sort();
  return pair[0] + ':' + pair[1];
}
function _chatIdMatches(ctx, dChatType, dChatId) {
  if (dChatType !== ctx.chatType) return false;
  if (ctx.chatType === 'pm') {
    // Server sends normalized pair, compare with our normalized version
    var norm = _normPmChatId(ctx.chatId);
    return dChatId === norm || dChatId === ctx.chatId;
  }
  return String(dChatId) === String(ctx.chatId);
}

// pinnedMessage — ответ на getPinnedMessage
// Сервер шлёт: { chatType, chatId, ...DB_row }
socket.on('pinnedMessage', function(d) {
  if (!d) return;
  var ctx = _chatCtx();
  if (!ctx) return;
  var dChatType = d.chatType || d.chat_type;
  var dChatId   = String(d.chatId  || d.chat_id || '');
  var dMsgId    = d.msg_id;
  var dMsgText  = d.msg_text;
  if (_chatIdMatches(ctx, dChatType, dChatId)) {
    if (dMsgId) _showPinBar(dMsgId, dMsgText);
    else        _hidePinBar();
  }
});

// messagePinned — broadcast когда кто-то закрепил
socket.on('messagePinned', function(d) {
  if (!d) return;
  var ctx = _chatCtx();
  if (ctx && _chatIdMatches(ctx, d.chatType, String(d.chatId))) {
    _showPinBar(d.msgId, d.msgText);
    showToast('📌 Сообщение закреплено');
  }
});

// messageUnpinned — broadcast
socket.on('messageUnpinned', function(d) {
  if (!d) return;
  var ctx = _chatCtx();
  if (ctx && _chatIdMatches(ctx, d.chatType, String(d.chatId))) {
    _hidePinBar();
    showToast('📌 Откреплено');
  }
});

// Загружаем закреп при открытии каждого чата
socket.on('messageHistory', function() { setTimeout(_loadPin, 300); });
socket.on('privateHistory', function() { setTimeout(_loadPin, 300); });
socket.on('roomData',        function() { setTimeout(_loadPin, 300); });


// ══════════════════════════════════════════════════════════════════════════
// 2. УПОМИНАНИЯ 🔔
// ══════════════════════════════════════════════════════════════════════════
var _mentions = [];
var _mentionOpen = false;

function _mentionBadge() {
  var b = document.getElementById('mentionBadge');
  if (!b) return;
  if (_mentions.length > 0) {
    b.textContent = _mentions.length > 9 ? '9+' : _mentions.length;
    b.style.display = 'flex';
  } else {
    b.style.display = 'none';
  }
}
function toggleMentionPanel() {
  _mentionOpen = !_mentionOpen;
  var p = document.getElementById('mentionPanel');
  if (!p) return;
  p.classList.toggle('show', _mentionOpen);
  if (_mentionOpen) _renderMentions();
}
window.toggleMentionPanel = toggleMentionPanel;

function _renderMentions() {
  var list = document.getElementById('mentionList');
  if (!list) return;
  if (!_mentions.length) {
    list.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-muted);font-size:13px;">Нет упоминаний</div>';
    return;
  }
  list.innerHTML = _mentions.slice(0, 40).map(function(m, i) {
    var chatLabel = m.chatType === 'general' ? '💬 Общий' : m.chatType === 'pm' ? ('👤 ' + escHtml(m.chatName||m.from)) : ('👥 ' + escHtml(m.chatName||'Группа'));
    var timeStr   = m.ts ? new Date(m.ts).toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'}) : '';
    return '<div onclick="_goMention(' + i + ')" style="padding:12px 16px;cursor:pointer;border-bottom:1px solid var(--border);transition:background 0.15s;" onmouseover="this.style.background=\'var(--bg-hover)\'" onmouseout="this.style.background=\'\'">'
      + '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:3px;">'
      +   '<span style="font-size:12px;font-weight:700;color:var(--accent);">' + escHtml(m.from) + '</span>'
      +   '<span style="font-size:11px;color:var(--text-muted);">' + chatLabel + ' · ' + timeStr + '</span>'
      + '</div>'
      + '<div style="font-size:13px;line-height:1.4;color:var(--text-primary);">' + escHtml((m.text||'').slice(0,120)) + '</div>'
      + '</div>';
  }).join('');
}
window._goMention = function(i) {
  var m = _mentions[i];
  if (!m) return;
  var p = document.getElementById('mentionPanel');
  if (p) p.classList.remove('show');
  _mentionOpen = false;
  if      (m.chatType === 'general') { if (typeof openGeneral === 'function') openGeneral(); }
  else if (m.chatType === 'pm')      { if (typeof openPM      === 'function') openPM(m.chatId, m.chatName || m.from); }
  else if (m.chatType === 'room')    { if (typeof openRoom     === 'function') openRoom(Number(m.chatId)); }
  setTimeout(function(){ if (m.msgId) _scrollToMsg(m.msgId); }, 700);
};
function clearMentions() { _mentions = []; _mentionBadge(); _renderMentions(); }
window.clearMentions = clearMentions;

// mentionReceived — сервер шлёт этот ивент только упоминаемому пользователю
socket.on('mentionReceived', function(d) {
  if (!d) return;
  _mentions.unshift({
    from:     d.from || '?',
    chatType: d.chatType || 'general',
    chatId:   String(d.chatId || 'general'),
    chatName: d.chatName || (d.chatType === 'general' ? 'Общий чат' : d.chatType === 'pm' ? (d.from || '?') : 'Группа'),
    msgId:    d.msgId,
    text:     d.text || '',
    ts:       d.ts || Date.now()
  });
  if (_mentions.length > 60) _mentions.pop();
  _mentionBadge();
  showToast('🔔 ' + (d.from || '?') + ' упомянул тебя');
  if (_mentionOpen) _renderMentions();
});

// Закрытие панели при клике вне
document.addEventListener('click', function(e) {
  if (!_mentionOpen) return;
  var p = document.getElementById('mentionPanel');
  var b = document.getElementById('mentionBtn');
  if (p && b && !p.contains(e.target) && !b.contains(e.target)) {
    p.classList.remove('show');
    _mentionOpen = false;
  }
});

// Подсветка @упоминаний в тексте
(function patchFmtText(){
  if (window._ftPatched) return;
  var orig = window.formatText;
  if (!orig) { setTimeout(patchFmtText, 500); return; }
  window._ftPatched = true;
  window.formatText = function(text) {
    return orig(text).replace(/@([a-zA-Zа-яА-ЯёЁ0-9_]+)/g, function(match, name) {
      var nl = name.toLowerCase();
      var isMe = (myLogin    && nl === myLogin.toLowerCase()) ||
                 (myNickname && nl === myNickname.toLowerCase().replace(/\s+/g,''));
      return isMe
        ? '<span style="background:rgba(255,200,50,0.25);color:#e6b800;border-radius:4px;padding:0 3px;font-weight:700;">' + match + '</span>'
        : '<span style="color:var(--accent);font-weight:600;">' + match + '</span>';
    });
  };
})();


// ══════════════════════════════════════════════════════════════════════════
// 3. ГЛОБАЛЬНЫЙ ПОИСК 🔍
// ══════════════════════════════════════════════════════════════════════════
var _gsCache = { general: [], pm: {}, room: {} };
socket.on('messageHistory', function(msgs) {
  if (Array.isArray(msgs)) _gsCache.general = msgs.map(function(m){ return {id:m.id,text:m.text||'',from:m.username||'',ts:m.timestamp}; });
});
socket.on('privateHistory', function(d) {
  if (!d || !d.otherLogin) return;
  _gsCache.pm[d.otherLogin] = { nick: d.otherNickname||d.otherLogin, msgs: (d.messages||[]).map(function(m){ return {id:m.id,text:m.text||'',from:m.from_nickname||m.from_login||'',ts:m.timestamp}; }) };
});
socket.on('roomData', function(d) {
  if (!d || !d.room) return;
  _gsCache.room[d.room.id] = { name: d.room.name, msgs: (d.messages||[]).map(function(m){ return {id:m.id,text:m.text||'',from:m.username||'',ts:m.timestamp}; }) };
});
socket.on('chatMessage',       function(m){ if(m&&m.id&&m.text) _gsCache.general.push({id:m.id,text:m.text,from:m.username||'',ts:m.timestamp}); });
socket.on('newPrivateMessage', function(m){ var k=m&&(m.from_login===myLogin?m.to_login:m.from_login); if(!k) return; if(!_gsCache.pm[k]) _gsCache.pm[k]={nick:m.from_nickname||k,msgs:[]}; if(m.id&&m.text) _gsCache.pm[k].msgs.push({id:m.id,text:m.text,from:m.from_nickname||'',ts:m.timestamp}); });
socket.on('roomNewMessage',    function(m){ var r=m&&m.room_id; if(!r) return; if(!_gsCache.room[r]) _gsCache.room[r]={name:'Группа',msgs:[]}; if(m.id&&m.text) _gsCache.room[r].msgs.push({id:m.id,text:m.text,from:m.username||'',ts:m.timestamp}); });

function openGlobalSearch() {
  var modal = document.getElementById('globalSearchModal');
  if (!modal) return;
  modal.classList.add('show');
  var inp = document.getElementById('globalSearchInput');
  if (inp) { inp.value = ''; inp.focus(); }
  var total = _gsCache.general.length
    + Object.keys(_gsCache.pm).reduce(function(s,k){return s+_gsCache.pm[k].msgs.length;},0)
    + Object.keys(_gsCache.room).reduce(function(s,k){return s+_gsCache.room[k].msgs.length;},0);
  var res = document.getElementById('globalSearchResults');
  if (res) res.innerHTML = '<div class="gs-empty">📦 Кэш: ' + total + ' сообщений. Начни вводить...</div>';
}
window.openGlobalSearch = openGlobalSearch;
function closeGlobalSearch() { var m=document.getElementById('globalSearchModal'); if(m) m.classList.remove('show'); }
window.closeGlobalSearch = closeGlobalSearch;
var _gsTimer = null;
function globalSearchQuery() {
  var q = (document.getElementById('globalSearchInput').value||'').trim();
  clearTimeout(_gsTimer);
  var res = document.getElementById('globalSearchResults');
  if (q.length < 2) { if(res) res.innerHTML='<div class="gs-empty">Минимум 2 символа</div>'; return; }
  if (res) res.innerHTML='<div class="gs-empty">🔍 Ищем...</div>';
  _gsTimer = setTimeout(function(){ _doGsSearch(q); }, 300);
}
window.globalSearchQuery = globalSearchQuery;
function _doGsSearch(q) {
  var ql = q.toLowerCase(), results = [];
  _gsCache.general.forEach(function(m){ if(m.text&&m.text.toLowerCase().includes(ql)) results.push({chatType:'general',chatId:'general',chatName:'💬 Общий чат',msgId:m.id,from:m.from,text:m.text,ts:m.ts}); });
  Object.keys(_gsCache.pm).forEach(function(l){ var e=_gsCache.pm[l]; (e.msgs||[]).forEach(function(m){ if(m.text&&m.text.toLowerCase().includes(ql)) results.push({chatType:'pm',chatId:l,chatName:'👤 '+(e.nick||l),msgId:m.id,from:m.from,text:m.text,ts:m.ts}); }); });
  Object.keys(_gsCache.room).forEach(function(r){ var e=_gsCache.room[r]; (e.msgs||[]).forEach(function(m){ if(m.text&&m.text.toLowerCase().includes(ql)) results.push({chatType:'room',chatId:r,chatName:'👥 '+(e.name||'Группа'),msgId:m.id,from:m.from,text:m.text,ts:m.ts}); }); });
  results.sort(function(a,b){ return (b.ts||0)-(a.ts||0); });
  var el = document.getElementById('globalSearchResults');
  if (!el) return;
  if (!results.length) { el.innerHTML='<div class="gs-empty">Ничего не найдено</div>'; return; }
  var re = new RegExp('('+q.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')','gi');
  el.innerHTML = results.slice(0,60).map(function(r,i){
    var hi = escHtml(r.text.slice(0,150)).replace(re,'<mark style="background:rgba(92,160,220,0.4);border-radius:2px;padding:0 1px;">$1</mark>');
    return '<div data-gsi="'+i+'" style="padding:10px 16px;cursor:pointer;border-bottom:1px solid var(--border);transition:background 0.15s;" onmouseover="this.style.background=\'var(--bg-hover)\'" onmouseout="this.style.background=\'\'">'
      + '<div style="font-size:11px;color:var(--accent);font-weight:700;margin-bottom:3px;">'+r.chatName+(r.from?' · '+escHtml(r.from):'')+'<span style="float:right;color:var(--text-muted);">'+(r.ts?new Date(r.ts).toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'}):'')+' </span></div>'
      + '<div style="font-size:13px;">'+hi+'</div></div>';
  }).join('');
  el.querySelectorAll('[data-gsi]').forEach(function(div){
    var r = results[parseInt(div.getAttribute('data-gsi'))];
    div.addEventListener('click', function(){
      closeGlobalSearch();
      if      (r.chatType==='general') { if(typeof openGeneral==='function') openGeneral(); }
      else if (r.chatType==='pm')      { if(typeof openPM==='function')      openPM(r.chatId, r.chatName.replace('👤 ','')); }
      else if (r.chatType==='room')    { if(typeof openRoom==='function')    openRoom(Number(r.chatId)); }
      setTimeout(function(){ _scrollToMsg(r.msgId); }, 600);
    });
  });
}
document.addEventListener('keydown', function(e){
  if((e.ctrlKey||e.metaKey)&&e.key==='k'){e.preventDefault();openGlobalSearch();}
  if(e.key==='Escape'){ closeGlobalSearch(); }
});


// ══════════════════════════════════════════════════════════════════════════
// 4. ПЕРЕВОД 🌍 — Google Translate unofficial API
// ══════════════════════════════════════════════════════════════════════════
var _tCache = {};
function translateMessage(msgId, origText) {
  var el = document.getElementById('msg-'+msgId) || document.getElementById('pm-'+msgId) || document.getElementById('rmsg-'+msgId);
  if (!el) return;
  var existing = el.querySelector('.msg-translation');
  if (existing) { existing.remove(); return; }
  var textEl = el.querySelector('.text');
  var text   = textEl ? (textEl.innerText||textEl.textContent).trim() : (origText||'');
  text = text.replace(/🌍.*$/gm,'').trim();
  if (!text) { showToast('Нечего переводить'); return; }
  if (_tCache[msgId]) { _insertTranslation(el, _tCache[msgId]); return; }
  showToast('🌍 Переводим...');
  var hasCyrillic = /[а-яёА-ЯЁ]/.test(text);
  var src = hasCyrillic ? 'ru' : 'en';
  var tgt = hasCyrillic ? 'en' : 'ru';
  var url = 'https://translate.googleapis.com/translate_a/single?client=gtx&sl='+src+'&tl='+tgt+'&dt=t&q='+encodeURIComponent(text.slice(0,500));
  fetch(url)
    .then(function(r){ if(!r.ok) throw new Error(r.status); return r.json(); })
    .then(function(data) {
      if (!data || !data[0]) throw new Error('empty');
      var translated = data[0].map(function(p){ return p[0]||''; }).join('');
      if (!translated.trim()) throw new Error('empty result');
      var result = '('+tgt.toUpperCase()+') ' + translated;
      _tCache[msgId] = result;
      _insertTranslation(el, result);
    })
    .catch(function(){ showToast('⚠️ Ошибка перевода'); });
}
window.translateMessage = translateMessage;
function _insertTranslation(el, text) {
  var div = document.createElement('div');
  div.className = 'msg-translation';
  div.style.cssText = 'font-size:12px;color:var(--text-secondary);font-style:italic;margin-top:6px;padding-top:5px;border-top:1px solid rgba(128,128,128,0.2);line-height:1.5;cursor:pointer;';
  div.title = 'Нажми чтобы скрыть';
  div.textContent = '🌍 ' + text;
  div.addEventListener('click', function(){ div.remove(); });
  var textEl = el.querySelector('.text');
  if (textEl) textEl.appendChild(div); else el.appendChild(div);
}


// ══════════════════════════════════════════════════════════════════════════
// 5. ЗВУКИ 🔔
// ══════════════════════════════════════════════════════════════════════════
var _sndType    = localStorage.getItem('notifSound') || 'ding';
var _sndEnabled = localStorage.getItem('notifSoundEnabled') !== 'false';
function playNotifSound() {
  if (!_sndEnabled) return;
  try {
    var Ctx = window.AudioContext || window.webkitAudioContext; if (!Ctx) return;
    var ctx=new Ctx(), osc=ctx.createOscillator(), gain=ctx.createGain();
    osc.connect(gain); gain.connect(ctx.destination);
    var freq={default:880,ding:1318,ping:1760,pop:523,chime:1047};
    osc.frequency.value = freq[_sndType]||1318;
    osc.type = _sndType==='pop'?'triangle':'sine';
    var dur = _sndType==='chime'?0.7:0.35;
    gain.gain.setValueAtTime(0.3,ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001,ctx.currentTime+dur);
    osc.start(); osc.stop(ctx.currentTime+dur);
  } catch(e){}
}
window.playNotifSound = playNotifSound;
function previewSound(type){ _sndType=type; localStorage.setItem('notifSound',type); playNotifSound(); }
window.previewSound = previewSound;
socket.on('chatMessage',       function(m){ if(m&&m.user_login!==myLogin) playNotifSound(); });
socket.on('newPrivateMessage', function(m){ if(m&&m.from_login!==myLogin) playNotifSound(); });
socket.on('roomNewMessage',    function(m){ if(m&&m.user_login!==myLogin) playNotifSound(); });


// ══════════════════════════════════════════════════════════════════════════
// 6. НОЧНОЙ РЕЖИМ 🌙
// ══════════════════════════════════════════════════════════════════════════
var _nightOn   = localStorage.getItem('nightScheduleEnabled') === 'true';
var _nightFrom = localStorage.getItem('nightFrom') || '22:00';
var _nightTo   = localStorage.getItem('nightTo')   || '08:00';
function _isNight() {
  if (!_nightOn) return false;
  var now = new Date();
  var cur = now.getHours()*60 + now.getMinutes();
  var f=_nightFrom.split(':'), t=_nightTo.split(':');
  var fmin=+f[0]*60+ +f[1], tmin=+t[0]*60+ +t[1];
  return fmin > tmin ? (cur>=fmin||cur<tmin) : (cur>=fmin&&cur<tmin);
}
function _applyNight() {
  var body = document.body;
  if (_isNight()) {
    if (!body.classList.contains('_nightAuto')) {
      body._preNightClass = body.className;
      body.classList.add('_nightAuto');
      body.classList.remove('theme-light');
      if (!body.classList.contains('theme-dark')) body.classList.add('theme-dark');
    }
  } else {
    if (body.classList.contains('_nightAuto')) {
      body.className = (body._preNightClass||'').replace('_nightAuto','').trim() || '';
    }
  }
}
// Патч applyTheme — восстанавливать ночной режим после смены темы
setTimeout(function(){
  if (window._applyThemePatched) return;
  var orig = window.applyTheme;
  if (!orig) return;
  window._applyThemePatched = true;
  window.applyTheme = function(key) {
    orig(key);
    if (_isNight()) {
      document.body.classList.add('_nightAuto');
      document.body.classList.remove('theme-light');
      if (!document.body.classList.contains('theme-dark')) document.body.classList.add('theme-dark');
    }
  };
}, 200);
setInterval(_applyNight, 30000);
setTimeout(_applyNight, 1500);

function openSoundNightSettings() {
  var body = document.getElementById('settingsBody');
  if (!body) return;
  _nightOn    = localStorage.getItem('nightScheduleEnabled') === 'true';
  _nightFrom  = localStorage.getItem('nightFrom') || '22:00';
  _nightTo    = localStorage.getItem('nightTo')   || '08:00';
  _sndEnabled = localStorage.getItem('notifSoundEnabled') !== 'false';
  _sndType    = localStorage.getItem('notifSound') || 'ding';
  var nowStr  = new Date().toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'});
  var sndLabels = {default:'Стандартный',ding:'Дзинь',ping:'Пинг',pop:'Мягкий',chime:'Колокольчик'};
  function mkTgl(id,on,fn) {
    return '<div id="'+id+'" onclick="'+fn+'" style="position:relative;width:44px;height:24px;min-width:44px;background:'+(on?'var(--accent)':'rgba(255,255,255,0.15)')+';border-radius:12px;cursor:pointer;transition:background 0.2s;flex-shrink:0;"><div style="position:absolute;top:2px;left:'+(on?'22px':'2px')+';width:20px;height:20px;background:#fff;border-radius:50%;transition:left 0.2s;box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div></div>';
  }
  body.innerHTML =
    '<div style="padding-bottom:24px;">'
    + '<div style="display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid var(--border);">'
    +   '<button onclick="renderSettingsMain()" style="background:transparent;border:none;color:var(--text-secondary);font-size:22px;cursor:pointer;width:36px;height:36px;display:flex;align-items:center;justify-content:center;border-radius:50%;">←</button>'
    +   '<h3 style="font-size:16px;font-weight:700;margin:0;">🔔 Звук и ночной режим</h3>'
    + '</div>'
    + '<div style="padding:16px 20px 8px;font-size:12px;font-weight:700;color:var(--text-muted);text-transform:uppercase;">Звуки уведомлений</div>'
    + '<div style="padding:0 20px;">'
    +   '<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 0;border-bottom:1px solid var(--border);"><span style="font-size:14px;">Включить звуки</span>' + mkTgl('_sndTgl',_sndEnabled,'window._toggleSnd(this)') + '</div>'
    + ['default','ding','ping','pop','chime'].map(function(s){
        var a=_sndType===s;
        return '<div style="display:flex;align-items:center;gap:12px;padding:11px 0;border-bottom:1px solid var(--border);">'
          + '<div onclick="_setSnd(\''+s+'\')" id="_sr_'+s+'" style="width:20px;height:20px;border-radius:50%;border:2px solid '+(a?'var(--accent)':'var(--border)')+';background:'+(a?'var(--accent)':'transparent')+';cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;">'+(a?'<div style="width:8px;height:8px;background:#fff;border-radius:50%;"></div>':'')+'</div>'
          + '<span style="flex:1;font-size:14px;">🔔 '+sndLabels[s]+'</span>'
          + '<button onclick="previewSound(\''+s+'\')" style="background:var(--bg-input);border:none;color:var(--text-primary);padding:6px 14px;border-radius:8px;cursor:pointer;font-size:12px;font-weight:600;">▶ Тест</button></div>';
      }).join('')
    + '</div>'
    + '<div style="padding:20px 20px 8px;font-size:12px;font-weight:700;color:var(--text-muted);text-transform:uppercase;">Ночной режим по расписанию</div>'
    + '<div style="padding:0 20px;">'
    +   '<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 0;border-bottom:1px solid var(--border);">'
    +     '<div><div style="font-size:14px;">Авто тёмная тема</div><div style="font-size:12px;color:var(--text-muted);margin-top:2px;">Сейчас '+nowStr+' — '+(_isNight()?'🌙 активен':'☀️ не активен')+'</div></div>'
    +     mkTgl('_nightTgl',_nightOn,'window._toggleNight(this)')
    +   '</div>'
    +   '<div style="display:flex;gap:16px;margin-top:16px;">'
    +     '<div style="flex:1;"><div style="font-size:12px;color:var(--text-muted);margin-bottom:6px;">Включить с</div><input type="time" id="_nFrom" value="'+_nightFrom+'" oninput="window._saveNightTimes()" style="width:100%;padding:10px;border:1.5px solid var(--border);border-radius:10px;background:var(--bg-input);color:var(--text-primary);font-size:15px;box-sizing:border-box;"></div>'
    +     '<div style="flex:1;"><div style="font-size:12px;color:var(--text-muted);margin-bottom:6px;">Выключить в</div><input type="time" id="_nTo" value="'+_nightTo+'" oninput="window._saveNightTimes()" style="width:100%;padding:10px;border:1.5px solid var(--border);border-radius:10px;background:var(--bg-input);color:var(--text-primary);font-size:15px;box-sizing:border-box;"></div>'
    +   '</div>'
    + '</div>'
    + '</div>';
  // Открываем оверлей НАПРЯМУЮ, не через openSettings()
  var overlay = document.getElementById('settingsOverlay');
  if (overlay) overlay.classList.add('show');
}
window.openSoundNightSettings = openSoundNightSettings;
window._toggleSnd = function(el) {
  _sndEnabled=!_sndEnabled; localStorage.setItem('notifSoundEnabled',String(_sndEnabled));
  el.style.background=_sndEnabled?'var(--accent)':'rgba(255,255,255,0.15)';
  var d=el.querySelector('div'); if(d) d.style.left=_sndEnabled?'22px':'2px';
};
window._setSnd = function(type) {
  _sndType=type; localStorage.setItem('notifSound',type);
  ['default','ding','ping','pop','chime'].forEach(function(s){
    var e=document.getElementById('_sr_'+s); if(!e) return;
    var a=s===type;
    e.style.border='2px solid '+(a?'var(--accent)':'var(--border)');
    e.style.background=a?'var(--accent)':'transparent';
    e.innerHTML=a?'<div style="width:8px;height:8px;background:#fff;border-radius:50%;"></div>':'';
  });
  playNotifSound();
};
window._toggleNight = function(el) {
  _nightOn=!_nightOn; localStorage.setItem('nightScheduleEnabled',String(_nightOn));
  el.style.background=_nightOn?'var(--accent)':'rgba(255,255,255,0.15)';
  var d=el.querySelector('div'); if(d) d.style.left=_nightOn?'22px':'2px';
  _applyNight();
  showToast(_nightOn?'🌙 Ночной режим включён':'☀️ Ночной режим выключен');
};
window._saveNightTimes = function() {
  var f=document.getElementById('_nFrom'), t=document.getElementById('_nTo');
  if(f){_nightFrom=f.value; localStorage.setItem('nightFrom',_nightFrom);}
  if(t){_nightTo=t.value;   localStorage.setItem('nightTo',_nightTo);}
  _applyNight();
};


// ══════════════════════════════════════════════════════════════════════════
// 7. АНАЛИТИКА ГРУПП 📊
// ══════════════════════════════════════════════════════════════════════════
function openRoomAnalytics(roomId) {
  var modal = document.getElementById('analyticsModal');
  if (!modal) return;
  modal.classList.add('show');
  var c = document.getElementById('analyticsContent');
  if (c) c.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-muted);">⏳ Загрузка...</div>';
  socket.emit('getRoomAnalytics', { roomId: Number(roomId) });
}
window.openRoomAnalytics = openRoomAnalytics;
function closeAnalytics() { var m=document.getElementById('analyticsModal'); if(m) m.classList.remove('show'); }
window.closeAnalytics = closeAnalytics;
socket.on('roomAnalytics', function(data) {
  var content = document.getElementById('analyticsContent');
  if (!content) return;
  var top = data.topUsers||[], maxC = top.length ? Math.max.apply(null,top.map(function(u){return u.msg_count;})) : 1;
  var html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px;">'
    + '<div style="background:var(--bg-input);border-radius:14px;padding:16px;text-align:center;"><div style="font-size:30px;font-weight:800;color:var(--accent);">'+(data.totalMessages||0)+'</div><div style="font-size:12px;color:var(--text-muted);margin-top:4px;">📨 Сообщений</div></div>'
    + '<div style="background:var(--bg-input);border-radius:14px;padding:16px;text-align:center;"><div style="font-size:30px;font-weight:800;color:var(--accent);">'+(data.totalMembers||0)+'</div><div style="font-size:12px;color:var(--text-muted);margin-top:4px;">👥 Участников</div></div>'
    + '</div>';
  if (data.dailyMessages&&data.dailyMessages.length) {
    var maxD=Math.max.apply(null,data.dailyMessages.map(function(d){return d.count;}))||1;
    html+='<div style="margin-bottom:20px;"><div style="font-size:12px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:10px;">📈 Активность 7 дней</div><div style="display:flex;align-items:flex-end;gap:4px;height:80px;">';
    data.dailyMessages.forEach(function(d){ var h=Math.max(3,Math.round((d.count/maxD)*64)); html+='<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px;"><div style="font-size:10px;color:var(--text-muted);">'+d.count+'</div><div style="width:100%;background:var(--accent);border-radius:3px 3px 0 0;height:'+h+'px;"></div><div style="font-size:9px;color:var(--text-muted);">'+escHtml(d.day)+'</div></div>'; });
    html+='</div></div>';
  }
  if (top.length) {
    html+='<div><div style="font-size:12px;font-weight:700;color:var(--text-muted);text-transform:uppercase;margin-bottom:12px;">🏆 Топ участников</div>';
    top.forEach(function(u,i){ var pct=Math.round((u.msg_count/maxC)*100); var medal=i===0?'🥇':i===1?'🥈':i===2?'🥉':'#'+(i+1); html+='<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:4px;"><span>'+medal+' '+escHtml(u.username)+'</span><span style="color:var(--text-muted);">'+u.msg_count+'</span></div><div style="height:5px;background:var(--bg-input);border-radius:3px;overflow:hidden;"><div style="height:100%;width:'+pct+'%;background:var(--accent);border-radius:3px;"></div></div></div>'; });
    html+='</div>';
  }
  content.innerHTML = html;
});


// ══════════════════════════════════════════════════════════════════════════
// 8. ФОТО 1 ПРОСМОТР 👁
// ══════════════════════════════════════════════════════════════════════════
var _voViewed={}, _voImages={};
function sendViewOncePhoto() {
  var inp=document.createElement('input'); inp.type='file'; inp.accept='image/*';
  inp.addEventListener('change', function() {
    var file=inp.files&&inp.files[0]; if(!file) return;
    if(file.size>15*1024*1024){showToast('Максимум 15 МБ');return;}
    var reader=new FileReader();
    reader.onload=function(ev){
      var payload={type:'view_once',image:ev.target.result,text:''};
      if     (currentView==='pm'&&currentPrivateLogin)  socket.emit('privateMessage',Object.assign({toLogin:currentPrivateLogin},payload));
      else if(currentView==='room'&&currentRoomId)      socket.emit('roomMessage',   Object.assign({roomId:currentRoomId},payload));
      else if(currentView==='general')                  socket.emit('chatMessage',   payload);
      showToast('👁 Отправлено — доступно 1 раз');
    };
    reader.readAsDataURL(file);
  });
  inp.click();
}
window.sendViewOncePhoto=sendViewOncePhoto;
function openViewOnce(msgId) {
  if(_voViewed[msgId]){showToast('Уже просмотрено');return;}
  var imgSrc=_voImages[msgId]; if(!imgSrc){showToast('Изображение недоступно');return;}
  _voViewed[msgId]=true;
  var overlay=document.createElement('div');
  overlay.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,0.97);z-index:10000;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;';
  var sec=10;
  var timerEl=document.createElement('div');
  timerEl.style.cssText='color:rgba(255,255,255,0.7);font-size:14px;font-weight:600;';
  timerEl.textContent='🔒 Закроется через '+sec+' сек';
  var img=document.createElement('img'); img.src=imgSrc;
  img.style.cssText='max-width:88vw;max-height:74vh;border-radius:12px;object-fit:contain;';
  var closeBtn=document.createElement('button'); closeBtn.textContent='✕ Закрыть';
  closeBtn.style.cssText='background:rgba(255,255,255,0.12);border:1px solid rgba(255,255,255,0.2);color:#fff;padding:10px 28px;border-radius:10px;cursor:pointer;font-size:14px;font-weight:600;';
  overlay.appendChild(timerEl); overlay.appendChild(img); overlay.appendChild(closeBtn);
  document.body.appendChild(overlay);
  var tick=setInterval(function(){sec--;timerEl.textContent='🔒 Закроется через '+sec+' сек';if(sec<=0){clearInterval(tick);done();}},1000);
  function done(){clearInterval(tick);overlay.remove();_updateVoUI(msgId);}
  closeBtn.onclick=done;
}
window.openViewOnce=openViewOnce;
function _updateVoUI(msgId){
  ['msg-','pm-','rmsg-'].forEach(function(p){
    var el=document.getElementById(p+msgId); if(!el) return;
    var btn=el.querySelector('[data-vo]');
    if(btn) btn.outerHTML='<div style="display:inline-flex;align-items:center;gap:8px;opacity:0.5;padding:10px 14px;border-radius:10px;background:var(--bg-input);">👁️ <span style="font-size:13px;">Просмотрено</span></div>';
  });
}
function _handleVoMsg(msg, prefix) {
  if(!msg||msg.type!=='view_once') return;
  if(msg.image) _voImages[msg.id]=msg.image;
  setTimeout(function(){
    var el=document.getElementById(prefix+msg.id); if(!el) return;
    var imgEl=el.querySelector('img');
    if(imgEl&&imgEl.src&&!_voImages[msg.id]) _voImages[msg.id]=imgEl.src;
    var isMine=el.classList.contains('mine');
    var photoWrap=imgEl?(imgEl.closest('.photo-img-wrap')||imgEl.closest('.msg-image')||imgEl.parentElement):null;
    if(photoWrap) photoWrap.style.display='none';
    var textDiv=el.querySelector('.text');
    if(!textDiv) return;
    if(isMine) {
      textDiv.innerHTML='<div style="display:inline-flex;align-items:center;gap:8px;background:rgba(92,160,220,0.12);padding:10px 14px;border-radius:10px;">👁️ <div><div style="font-size:13px;font-weight:600;">Отправлено (1 просмотр)</div><div style="font-size:11px;color:var(--text-muted);">Получатель увидит 1 раз</div></div></div>';
    } else {
      textDiv.innerHTML='<div data-vo="1" onclick="openViewOnce('+msg.id+')" style="display:inline-flex;align-items:center;gap:10px;background:rgba(92,160,220,0.15);padding:12px 16px;border-radius:12px;cursor:pointer;border:1.5px solid var(--accent);">👁️ <div><div style="font-size:13px;font-weight:600;">Фото — нажми для просмотра</div><div style="font-size:11px;color:var(--text-muted);">10 сек · только 1 раз</div></div></div>';
    }
  }, 200);
}
socket.on('chatMessage',       function(m){ _handleVoMsg(m,'msg-'); });
socket.on('newPrivateMessage', function(m){ _handleVoMsg(m,'pm-'); });
socket.on('roomNewMessage',    function(m){ _handleVoMsg(m,'rmsg-'); });
function _patchVoHistory(msgs, prefix) {
  if(!Array.isArray(msgs)) return;
  msgs.forEach(function(msg){ if(msg.type==='view_once'){if(msg.image)_voImages[msg.id]=msg.image; setTimeout(function(){_handleVoMsg(msg,prefix);},600); }});
}
socket.on('messageHistory', function(msgs){ _patchVoHistory(msgs,'msg-'); });
socket.on('privateHistory', function(d){ if(d&&d.messages) _patchVoHistory(d.messages,'pm-'); });
socket.on('roomData',       function(d){ if(d&&d.messages) _patchVoHistory(d.messages,'rmsg-'); });

window.addEventListener('load', function(){
  setTimeout(function(){
    var inputArea=document.getElementById('inputArea');
    if(!inputArea||inputArea.querySelector('[data-vo-btn]')) return;
    var btn=document.createElement('button');
    btn.setAttribute('data-vo-btn','1');
    btn.className='ia-btn'; btn.title='Фото (1 просмотр)'; btn.textContent='👁';
    btn.addEventListener('click', sendViewOncePhoto);
    var sendBtn=document.getElementById('sendBtn');
    if(sendBtn) inputArea.insertBefore(btn,sendBtn); else inputArea.appendChild(btn);
  }, 800);
});





// ── GIF SEARCH ────────────────────────────────────────────
var gifSearchTimer = null;
window.searchGifs = function(query) {
  clearTimeout(gifSearchTimer);
  gifSearchTimer = setTimeout(function() {
    var grid = document.getElementById('emojiGrid');
    if (!query || !query.trim()) {
      grid.innerHTML = '<div style="color:var(--text-muted);font-size:13px;padding:10px;text-align:center;">Введи запрос для поиска GIF 🎬</div>';
      return;
    }
    grid.innerHTML = '<div style="color:var(--text-muted);font-size:13px;padding:10px;text-align:center;">⏳ Ищем...</div>';
    var q = encodeURIComponent(query.trim());
    // Tenor v2 — работает с публичным ключом
    var tenorKey = 'AIzaSyAyimkuYQYF_FXVALexPuGQctUWRURdCYQ';
    fetch('https://tenor.googleapis.com/v2/search?q=' + q + '&key=' + tenorKey + '&limit=20&media_filter=gif&contentfilter=medium')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (!data.results || !data.results.length) {
          // Fallback: Giphy public beta key
          return fetch('https://api.giphy.com/v1/gifs/search?api_key=0UTRbFtkMxAplrohufYco5IY74U8hOes&q=' + q + '&limit=20&rating=g')
            .then(function(r2) { return r2.json(); })
            .then(function(d2) {
              if (!d2.data || !d2.data.length) {
                grid.innerHTML = '<div style="color:var(--text-muted);font-size:13px;padding:10px;text-align:center;">GIF не найдены 😔</div>';
                return;
              }
              var html = '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:4px;">';
              d2.data.forEach(function(gif) {
                var thumb = gif.images && gif.images.fixed_width_small ? gif.images.fixed_width_small.url : null;
                var full = gif.images && gif.images.original ? gif.images.original.url : thumb;
                if (!thumb) return;
                html += '<img src="' + thumb + '" data-full="' + full + '" loading="lazy" style="width:100%;height:80px;object-fit:cover;border-radius:10px;cursor:pointer;" onclick="sendGif(this.dataset.full)" onerror="this.style.display=\'none\'">';
              });
              html += '</div>';
              grid.innerHTML = html;
            });
        }
        var html = '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:4px;">';
        data.results.forEach(function(item) {
          var med = item.media_formats;
          var thumb = med && med.tinygif ? med.tinygif.url : (med && med.gif ? med.gif.url : null);
          var full = med && med.gif ? med.gif.url : thumb;
          if (!thumb) return;
          html += '<img src="' + thumb + '" data-full="' + full + '" loading="lazy" style="width:100%;height:80px;object-fit:cover;border-radius:10px;cursor:pointer;" onclick="sendGif(this.dataset.full)" onerror="this.style.display=\'none\'">';
        });
        html += '</div>';
        grid.innerHTML = html;
      })
      .catch(function() {
        grid.innerHTML = '<div style="color:var(--text-muted);font-size:13px;padding:10px;text-align:center;">Ошибка загрузки 😔</div>';
      });
  }, 400);
};

window.sendGif = function(gifUrl) {
  if (!gifUrl) return;
  if (currentView === 'ghost' && _ghostRoomId) {
    socket.emit('ghostMessage', { roomId: _ghostRoomId, image: gifUrl, type: 'image', anon: _ghostAnon });
  } else if (currentView === 'pm' && currentPrivateLogin) {
    socket.emit('privateMessage', { toLogin: currentPrivateLogin, image: gifUrl, type: 'image' });
  } else if (currentView === 'room' && currentRoomId) {
    socket.emit('roomMessage', { roomId: currentRoomId, image: gifUrl, type: 'image' });
  } else if (currentView === 'general') {
    socket.emit('chatMessage', { image: gifUrl, type: 'image' });
  }
  document.getElementById('emojiPanel').style.display = 'none';
  emojiPanelOpen = false;
};

// ── PHOTO STICKER ─────────────────────────────────────────
var photoStickerData = null;

window.openPhotoStickerModal = function() {
  photoStickerData = null;
  document.getElementById('stickerPreview').innerHTML = '📷';
  document.getElementById('stickerFileInput').value = '';
  document.getElementById('sendStickerBtn').disabled = true;
  document.getElementById('sendStickerBtn').style.opacity = '0.5';
  var panel = document.getElementById('emojiPanel');
  if (panel) panel.style.display = 'none';
  emojiPanelOpen = false;
  setTimeout(function() {
    document.getElementById('photoStickerModal').classList.add('show');
  }, 50);
};

window.closePhotoStickerModal = function() {
  document.getElementById('photoStickerModal').classList.remove('show');
};

window.previewSticker = function(input) {
  var file = input.files[0];
  if (!file) return;
  if (file.size > 5 * 1024 * 1024) { alert('Фото слишком большое (макс 5МБ)'); return; }
  var reader = new FileReader();
  reader.onload = function(e) {
    photoStickerData = e.target.result;
    var img = document.createElement('img');
    img.src = photoStickerData;
    img.style = 'width:100%;height:100%;object-fit:cover;border-radius:18px;';
    document.getElementById('stickerPreview').innerHTML = '';
    document.getElementById('stickerPreview').appendChild(img);
    document.getElementById('sendStickerBtn').disabled = false;
    document.getElementById('sendStickerBtn').style.opacity = '1';
  };
  reader.readAsDataURL(file);
};

window.sendPhotoSticker = function() {
  if (!photoStickerData) return;
  if (currentView === 'pm' && currentPrivateLogin) {
    socket.emit('privateMessage', { toLogin: currentPrivateLogin, image: photoStickerData, type: 'photo_sticker' });
  } else if (currentView === 'room' && currentRoomId) {
    socket.emit('roomMessage', { roomId: currentRoomId, image: photoStickerData, type: 'photo_sticker' });
  } else if (currentView === 'general') {
    socket.emit('chatMessage', { image: photoStickerData, type: 'photo_sticker' });
  }
  window.closePhotoStickerModal();
};

// ── ATTACH EMAIL ──────────────────────────────────────────
window.showAttachEmailModal = function() {
  document.getElementById('attachEmailStep1').style.display = '';
  document.getElementById('attachEmailStep2').style.display = 'none';
  document.getElementById('attachEmailStepSuccess').style.display = 'none';
  document.getElementById('attachEmailError').textContent = '';
  document.getElementById('attachEmailInput').value = '';
  document.getElementById('attachEmailCode').value = '';
  document.getElementById('attachEmailModal').classList.add('show');
};

window.closeAttachEmailModal = function() {
  document.getElementById('attachEmailModal').classList.remove('show');
};

window.dismissAttachEmail = function() {
  localStorage.setItem('attachEmailDismissed_' + myLogin, '1');
  window.closeAttachEmailModal();
};

window.submitAttachEmail = function() {
  var email = document.getElementById('attachEmailInput').value.trim();
  if (!email) return;
  socket.emit('requestAttachEmail', { email: email });
};

window.submitAttachEmailCode = function() {
  var code = document.getElementById('attachEmailCode').value.trim();
  if (!code) return;
  socket.emit('confirmAttachEmail', { code: code });
};

socket.on('attachEmailCodeSent', function() {
  document.getElementById('attachEmailStep1').style.display = 'none';
  document.getElementById('attachEmailStep2').style.display = '';
  document.getElementById('attachEmailError').textContent = '';
});

socket.on('emailAttached', function(data) {
  document.getElementById('attachEmailStep2').style.display = 'none';
  document.getElementById('attachEmailStepSuccess').style.display = '';
  var desc = document.getElementById('settingsEmailDesc');
  if (desc) desc.textContent = data.email;
});

socket.on('attachEmailError', function(msg) {
  document.getElementById('attachEmailError').textContent = msg;
});


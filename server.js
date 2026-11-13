'use strict';
/**
 * Price Tracker central server. No dependencies, just Node 14+.
 *
 *   GET  /               status text
 *   GET  /prices?since=N every item changed after server-time N  ->  {"now":ms,"items":[...]}
 *   POST /submit         [{"name","source":"AH|BZ","category","price","sell"}, ...]  (max 300 per call)
 *
 * Anyone can upload (as requested), but there are basic safety limits:
 * request size, items per request, price range, name rules, per-IP rate limit and a database size cap.
 */
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');

process.on('uncaughtException', e => console.error('UNCAUGHT ERROR (server kept running):', e && e.stack || e));
process.on('unhandledRejection', e => console.error('UNHANDLED REJECTION (server kept running):', e && e.stack || e));

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');

const MAX_BODY = 256 * 1024;       // bytes per request
const MAX_ITEMS_PER_POST = 300;
const MAX_DB_ITEMS = 60000;
const MAX_HISTORY = 100;
const MAX_PRICE = 1e12;
const RATE_PER_MIN = 120;          // requests per IP per minute
const CATEGORIES = new Set(['Weapons', 'Armor', 'Accessories', 'Tools', 'Pets', 'Enchanted Books',
  'Dungeon Items', 'Enchanted Materials', 'Raw Materials', 'Other']);

// ---------- storage ----------
let db = {};            // key -> item
let dirty = false;

// Optional permanent backup in Upstash Redis (free). Survives Render restarts / redeploys.
// Turned on by the env vars UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.
const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const useRemote = !!(REDIS_URL && REDIS_TOKEN);
const CHUNK = 250;                 // items per Redis value (keeps every request small)
const REMOTE_EVERY_MS = 120000;    // remote save at most every 2 minutes (free plan has a monthly command limit)
let remoteReady = false;           // true once Redis was read (or is known to be empty) -> safe to overwrite it
let remoteGen = 0, remoteChunks = 0, lastRemote = 0;
if (useRemote) console.log('Upstash Redis backup ON');

async function redis(cmd) {
  if (typeof fetch !== 'function') throw new Error('this Node version has no fetch - set Node 18 or newer in Render');
  const r = await fetch(REDIS_URL, {
    signal: (typeof AbortSignal !== 'undefined' && AbortSignal.timeout) ? AbortSignal.timeout(15000) : undefined,
    method: 'POST',
    headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd)
  });
  if (!r.ok) {
    let t = ''; try { t = (await r.text()).slice(0, 200); } catch (e) { /* ignore */ }
    throw new Error('Redis HTTP ' + r.status + ' ' + t);
  }
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}

function mergeDb(remote) {            // keep whichever copy of an item is newer
  let n = 0;
  for (const k in remote) {
    if (!db[k] || (remote[k].lastSeen || 0) > (db[k].lastSeen || 0)) { db[k] = remote[k]; n++; }
  }
  return n;
}

function loadLocal() {
  try { mergeDb(JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) || {}); } catch (e) { /* no local file yet */ }
}

async function loadRemote() {
  if (!useRemote || remoteReady) return;
  try {
    const metaText = await redis(['GET', 'pt:meta']);
    if (metaText === null) { remoteReady = true; dirty = true; console.log('Upstash is empty - starting fresh'); return; }
    const meta = JSON.parse(metaText);
    const all = {};
    for (let i = 0; i < meta.chunks; i++) {
      const t = await redis(['GET', 'pt:' + meta.gen + ':' + i]);
      if (t === null) throw new Error('missing chunk ' + i);
      Object.assign(all, JSON.parse(t));
    }
    const n = mergeDb(all);
    remoteGen = meta.gen; remoteChunks = meta.chunks;
    remoteReady = true; dirty = true;
    console.log('Loaded ' + n + ' items from Upstash');
  } catch (e) {
    console.error('Upstash load failed (will retry, not overwriting it):', e.message);
  }
}

async function saveRemote() {
  const keys = Object.keys(db);
  const chunks = [];
  for (let i = 0; i < keys.length; i += CHUNK) {
    const part = {};
    for (const k of keys.slice(i, i + CHUNK)) part[k] = db[k];
    chunks.push(JSON.stringify(part));
  }
  const gen = Date.now();
  for (let i = 0; i < chunks.length; i++) await redis(['SET', 'pt:' + gen + ':' + i, chunks[i]]);
  await redis(['SET', 'pt:meta', JSON.stringify({ gen, chunks: chunks.length })]);   // switch to the new copy
  if (remoteChunks > 0) {                                                          // remove the old copy
    const old = []; for (let i = 0; i < remoteChunks; i++) old.push('pt:' + remoteGen + ':' + i);
    try { await redis(['DEL'].concat(old)); } catch (e) { /* not important */ }
  }
  remoteGen = gen; remoteChunks = chunks.length; lastRemote = Date.now();
  console.log('Saved ' + keys.length + ' items to Upstash (' + chunks.length + ' chunks)');
}

let saving = false;
async function save(force) {
  if (!dirty || saving) return;
  saving = true;
  try {
    try {
      const tmp = DATA_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(db));
      fs.renameSync(tmp, DATA_FILE);
    } catch (e) { console.error('local save failed', e.message); }
    if (useRemote) {
      if (remoteReady && (force || Date.now() - lastRemote >= REMOTE_EVERY_MS)) {
        await saveRemote();
        dirty = false;
      }
    } else dirty = false;
  } catch (e) {
    console.error('Upstash SAVE FAILED (nothing was stored): ' + e.message);
    lastRemote = Date.now();          // wait 2 minutes before trying again (saves your free command limit)
  }
  saving = false;
}
setInterval(save, 30000).unref();
setInterval(() => { if (useRemote && !remoteReady) loadRemote(); }, 60000).unref();
async function shutdown() {
  const t = setTimeout(() => process.exit(0), 8000);   // never hang on exit
  await save(true);
  clearTimeout(t);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

const keyOf = (name, source) => (source === 'BZ' ? 'bz:' : '') + name.toLowerCase();

// ---------- rate limit ----------
const hits = new Map();
function limited(ip) {
  const now = Date.now();
  let h = hits.get(ip);
  if (!h || now > h.reset) { h = { n: 0, reset: now + 60000 }; hits.set(ip, h); }
  return ++h.n > RATE_PER_MIN;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (now > v.reset) hits.delete(k); }, 60000).unref();

// ---------- logic ----------
const UI_RE = /(% of spread|best offer|best order|same as best|top order|^custom |sell inventory|fill my inventory|buy only one|buy instantly|sell instantly|^create |^view |^click |^go back|^manage | setup$)/i;
const stats = { posts: 0, accepted: 0, rejected: 0, lastUpload: 0, lastError: '' };
const recent = [];                 // last uploads for the admin panel
const uploaders = new Map();       // hashed ip -> last seen (unique uploaders)
function purgeBad() {              // remove menu buttons and impossible data
  let n = 0;
  for (const k in db) {
    const it = db[k];
    if (UI_RE.test(it.name) || !(it.lowestBin > 0) || (it.source === 'BZ' && it.sellPrice > 0 && (it.lowestBin / it.sellPrice - 1) > 3)) { delete db[k]; n++; }
  }
  if (n) dirty = true;
  return n;
}
function accept(e, now) {
  if (!e || typeof e !== 'object') return false;
  let name = typeof e.name === 'string' ? e.name.trim() : '';
  if (name.length < 2 || name.length > 80 || /[\u0000-\u001f<>]/.test(name)) return false;
  if (UI_RE.test(name)) return false;
  const source = e.source === 'BZ' ? 'BZ' : 'AH';
  const price = Number(e.price), sell = Number(e.sell || 0);
  if (!isFinite(price) || price <= 0 || price > MAX_PRICE) return false;
  const sellOk = isFinite(sell) && sell > 0 && sell <= MAX_PRICE;
  const category = CATEGORIES.has(e.category) ? e.category : 'Other';

  const key = keyOf(name, source);
  let it = db[key];
  if (!it) {
    if (Object.keys(db).length >= MAX_DB_ITEMS) return false;
    it = db[key] = { name, source, category: 'Other', lowestBin: 0, sellPrice: 0, avgPrice: 0, history: [], lastSeen: 0 };
  }
  // a player's own price history (sent with the one-time upload) fills the graphs / risers / fallers at once
  if (Array.isArray(e.history) && e.history.length > it.history.length) {
    const h = e.history.slice(-MAX_HISTORY).map(Number).filter(v => isFinite(v) && v > 0 && v <= MAX_PRICE);
    if (h.length > it.history.length) it.history = h;
  }
  if (it.history[it.history.length - 1] !== price) it.history.push(price);
  while (it.history.length > MAX_HISTORY) it.history.shift();
  it.lowestBin = price;
  if (sellOk) it.sellPrice = sell;
  it.avgPrice = it.history.reduce((a, b) => a + b, 0) / it.history.length;
  if (category !== 'Other' || !it.category) it.category = category;
  it.lastSeen = now;
  return true;
}

// ---------- http ----------
function send(res, code, body, type) {
  const data = typeof body === 'string' ? body : JSON.stringify(body);
  res.writeHead(code, { 'Content-Type': type || 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(data);
}

// ---------- admin panel (needs the env var ADMIN_PASSWORD) ----------
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';
const sessions = new Map();          // token -> expiry
const fails = new Map();             // ip -> {n, reset}
const same = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const bootTime = Date.now();

function readJson(req, cb) {
  let size = 0; const chunks = [];
  req.on('data', c => { size += c.length; if (size > MAX_BODY) req.destroy(); else chunks.push(c); });
  req.on('end', () => { let o = {}; try { o = JSON.parse(Buffer.concat(chunks).toString('utf8')) || {}; } catch (e) { /* empty */ } cb(o); });
}

function adminApi(req, res, url, ip) {
  if (!ADMIN_PASSWORD && !ADMIN_EMAIL) return send(res, 503, { error: 'Admin is off. Set ADMIN_PASSWORD (or ADMIN_EMAIL) in Render.' });
  const route = url.pathname.slice('/admin/api/'.length);
  if (route === 'login' && req.method === 'POST') {
    if (!ADMIN_PASSWORD) return send(res, 503, { error: 'Password login is off. Sign in with Google.' });
    return readJson(req, b => {
      const now = Date.now(); let f = fails.get(ip);
      if (!f || now > f.reset) { f = { n: 0, reset: now + 600000 }; fails.set(ip, f); }
      if (f.n >= 5) return send(res, 429, { error: 'Too many tries. Wait 10 minutes.' });
      if (!same(b.password || '', ADMIN_PASSWORD)) { f.n++; return send(res, 401, { error: 'Wrong password' }); }
      const token = crypto.randomBytes(24).toString('hex');
      sessions.set(token, now + 12 * 3600000);
      send(res, 200, { token });
    });
  }
  const tok = req.headers['x-admin-token'];
  const gs = readSession(tok);
  const isGoogleAdmin = !!(gs && ADMIN_EMAIL && gs.email === ADMIN_EMAIL);
  if (!isGoogleAdmin && (!tok || !sessions.has(tok) || sessions.get(tok) < Date.now())) return send(res, 401, { error: 'login needed' });

  if (route === 'overview') {
    let ah = 0, bz = 0, bad = 0;
    for (const k in db) { const it = db[k]; if (it.source === 'BZ') bz++; else ah++; if (UI_RE.test(it.name) || (it.source === 'BZ' && it.sellPrice > 0 && it.lowestBin / it.sellPrice > 4)) bad++; }
    const day = Date.now() - 86400000; let u = 0; for (const v of uploaders.values()) if (v > day) u++;
    return send(res, 200, { total: ah + bz, ah, bz, suspicious: bad, uploaders24h: u, uptimeMin: Math.round((Date.now() - bootTime) / 60000),
      stats, recent, storage: { remote: useRemote, ready: useRemote ? remoteReady : null, lastSave: lastRemote, dirty } });
  }
  if (route === 'items') {
    const q = (url.searchParams.get('q') || '').toLowerCase(), src = url.searchParams.get('source') || '';
    const only = url.searchParams.get('bad') === '1', page = Math.max(0, Number(url.searchParams.get('page')) || 0);
    let list = [];
    for (const k in db) {
      const it = db[k];
      if (src && it.source !== src) continue;
      if (q && !it.name.toLowerCase().includes(q)) continue;
      const margin = it.source === 'BZ' && it.sellPrice > 0 ? (it.lowestBin / it.sellPrice - 1) * 100 : 0;
      const flag = UI_RE.test(it.name) ? 'menu button' : margin > 100 ? 'huge margin' : '';
      if (only && !flag) continue;
      list.push({ key: k, name: it.name, source: it.source, price: it.lowestBin, sell: it.sellPrice, margin: Math.round(margin), flag, seen: it.lastSeen, hist: it.history.length });
    }
    list.sort((a, b) => b.seen - a.seen);
    return send(res, 200, { total: list.length, items: list.slice(page * 50, page * 50 + 50) });
  }
  if (route === 'export') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="pricetracker-backup.json"' });
    return res.end(JSON.stringify(db));
  }
  if (req.method !== 'POST') return send(res, 405, { error: 'POST only' });
  return readJson(req, b => {
    if (route === 'delete') { let n = 0; for (const k of (Array.isArray(b.keys) ? b.keys : [])) if (db[k]) { delete db[k]; n++; } if (n) dirty = true; return send(res, 200, { ok: true, removed: n }); }
    if (route === 'purge') return send(res, 200, { ok: true, removed: purgeBad() });
    if (route === 'set') {
      const it = db[b.key], p = Number(b.price), sl = Number(b.sell || 0);
      if (!it || !(p > 0) || p > MAX_PRICE) return send(res, 400, { error: 'bad value' });
      it.lowestBin = p; if (sl >= 0 && sl <= MAX_PRICE) it.sellPrice = sl; it.lastSeen = Date.now(); dirty = true;
      return send(res, 200, { ok: true });
    }
    if (route === 'save') { dirty = true; return save(true).then(() => send(res, 200, { ok: true })); }
    if (route === 'clear') {
      if (b.confirm !== 'DELETE ALL') return send(res, 400, { error: 'type DELETE ALL' });
      const n = Object.keys(db).length; db = {}; dirty = true; return send(res, 200, { ok: true, removed: n });
    }
    send(res, 404, { error: 'not found' });
  });
}

// ---------- Google sign-in + per-user watchlist (needs GOOGLE_CLIENT_ID and SESSION_SECRET) ----------
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const SESSION_SECRET = process.env.SESSION_SECRET || '';
const ADMIN_EMAIL = (process.env.ADMIN_EMAIL || '').trim().toLowerCase();
const CERTS_URL = process.env.GOOGLE_CERTS_URL || 'https://www.googleapis.com/oauth2/v3/certs';
const USERS_FILE = DATA_FILE + '.users';
let certs = { at: 0, keys: [] }, usersCache = null;

async function googleKeys() {
  if (certs.keys.length && Date.now() - certs.at < 3600000) return certs.keys;
  const j = await (await fetch(CERTS_URL)).json();
  certs = { at: Date.now(), keys: j.keys || [] };
  return certs.keys;
}
async function verifyGoogle(idToken) {
  const [h, p, sig] = String(idToken || '').split('.');
  if (!sig) throw new Error('bad token');
  const head = JSON.parse(Buffer.from(h, 'base64url')), pay = JSON.parse(Buffer.from(p, 'base64url'));
  const jwk = (await googleKeys()).find(k => k.kid === head.kid);
  if (!jwk || head.alg !== 'RS256') throw new Error('unknown key');
  const ok = crypto.verify('RSA-SHA256', Buffer.from(h + '.' + p), crypto.createPublicKey({ key: jwk, format: 'jwk' }), Buffer.from(sig, 'base64url'));
  if (!ok) throw new Error('bad signature');
  if (pay.iss !== 'accounts.google.com' && pay.iss !== 'https://accounts.google.com') throw new Error('bad issuer');
  if (pay.aud !== GOOGLE_CLIENT_ID) throw new Error('wrong client id');
  if (!(pay.exp * 1000 > Date.now())) throw new Error('token expired');
  if (!pay.email_verified || !pay.sub) throw new Error('email not verified');
  return pay;
}
const b64u = o => Buffer.from(JSON.stringify(o)).toString('base64url');
const mac = t => crypto.createHmac('sha256', SESSION_SECRET).update(t).digest('base64url');
function signSession(o) { const t = b64u(o); return t + '.' + mac(t); }
function readSession(tok) {
  if (!SESSION_SECRET || !tok) return null;
  const [t, m] = String(tok).split('.');
  if (!m || !same(m, mac(t))) return null;
  try { const o = JSON.parse(Buffer.from(t, 'base64url')); return o.exp > Date.now() ? o : null; } catch (e) { return null; }
}
function usersLocal() {
  if (!usersCache) { try { usersCache = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8')) || {}; } catch (e) { usersCache = {}; } }
  return usersCache;
}
async function getUser(sub) {
  const u = usersLocal();
  if (u[sub]) return u[sub];
  if (useRemote) { try { const t = await redis(['GET', 'pt:u:' + sub]); if (t) { u[sub] = JSON.parse(t); return u[sub]; } } catch (e) { /* ignore */ } }
  return null;
}
async function putUser(sub, rec) {
  usersLocal()[sub] = rec;
  try { fs.writeFileSync(USERS_FILE, JSON.stringify(usersCache)); } catch (e) { /* ignore */ }
  if (useRemote) { try { await redis(['SET', 'pt:u:' + sub, JSON.stringify(rec)]); } catch (e) { console.error('user save failed: ' + e.message); } }
}
function changeWatch(w, b) {            // one person's own watchlist: { "bz:coal": {above, below} }
  const key = typeof b.key === 'string' ? b.key.slice(0, 100) : '';
  if (!key) return;
  const n = v => { v = Number(v); return v > 0 && v <= MAX_PRICE ? v : 0; };
  const c = w[key] || { above: 0, below: 0 }; let on = !!w[key];
  if (b.watched !== undefined) { on = !!b.watched; if (!on) { c.above = 0; c.below = 0; } }
  if (b.above !== undefined) c.above = n(b.above);
  if (b.below !== undefined) c.below = n(b.below);
  if ((b.above !== undefined || b.below !== undefined) && (c.above > 0 || c.below > 0) && b.watched !== false) on = true;
  if (on && (w[key] || Object.keys(w).length < 300)) w[key] = c; else delete w[key];
}
const profile = (rec, email) => ({ email: rec.email, name: rec.name || '', picture: rec.picture || '', watch: rec.watch, admin: !!ADMIN_EMAIL && email === ADMIN_EMAIL });

function userApi(req, res, url) {
  const on = !!(GOOGLE_CLIENT_ID && SESSION_SECRET);
  if (url.pathname === '/api/config') return send(res, 200, { enabled: on, googleClientId: on ? GOOGLE_CLIENT_ID : '' });
  if (!on) return send(res, 503, { error: 'Sign-in is off. Set GOOGLE_CLIENT_ID and SESSION_SECRET in Render.' });
  if (url.pathname === '/api/login/google' && req.method === 'POST') {
    return readJson(req, async b => {
      try {
        const g = await verifyGoogle(b.credential);
        const email = String(g.email).toLowerCase();
        const rec = (await getUser(g.sub)) || { watch: {} };
        rec.email = email; rec.name = g.name || ''; rec.picture = g.picture || '';
        if (b.merge && typeof b.merge === 'object') {          // watchlist made before login (kept in the browser)
          for (const k of Object.keys(b.merge).slice(0, 300)) if (!rec.watch[k]) changeWatch(rec.watch, { key: k, watched: true, above: b.merge[k].above, below: b.merge[k].below });
        }
        await putUser(g.sub, rec);
        send(res, 200, { token: signSession({ sub: g.sub, email, exp: Date.now() + 30 * 86400000 }), user: profile(rec, email) });
      } catch (e) { send(res, 401, { error: 'Google sign-in failed: ' + e.message }); }
    });
  }
  const sess = readSession((req.headers.authorization || '').replace(/^Bearer /, ''));
  if (!sess) return send(res, 401, { error: 'sign in again' });
  if (url.pathname === '/api/me' && req.method === 'GET') {
    return getUser(sess.sub).then(rec => rec ? send(res, 200, profile(rec, sess.email)) : send(res, 401, { error: 'unknown user' }));
  }
  if (url.pathname === '/api/me/watch' && req.method === 'POST') {
    return readJson(req, async b => {
      const rec = await getUser(sess.sub);
      if (!rec) return send(res, 401, { error: 'unknown user' });
      changeWatch(rec.watch, b); await putUser(sess.sub, rec);
      send(res, 200, { ok: true, watch: rec.watch });
    });
  }
  send(res, 404, { error: 'not found' });
}

const server = http.createServer((req, res) => {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (limited(ip)) return send(res, 429, { error: 'slow down' });

  const url = new URL(req.url, 'http://x');

  // website (the same dashboard the mod shows locally)
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(html);
    } catch (e) {
      return send(res, 200, 'Price Tracker server OK - ' + Object.keys(db).length + ' items\n', 'text/plain; charset=utf-8');
    }
  }
  if (req.method === 'GET' && url.pathname === '/health') {
    return send(res, 200, 'Price Tracker server OK - ' + Object.keys(db).length + ' items\n', 'text/plain; charset=utf-8');
  }
  if (req.method === 'GET' && url.pathname === '/api/prices') {
    return send(res, 200, Object.values(db));
  }
  if (req.method === 'GET' && url.pathname === '/api/status') {
    let ah = 0, bz = 0, last = 0;
    for (const k in db) { if (db[k].source === 'BZ') bz++; else ah++; if (db[k].lastSeen > last) last = db[k].lastSeen; }
    return send(res, 200, { total: ah + bz, ah, bz, watched: 0, lastScan: last, now: Date.now(),
      settings: { tooltips: true, chestCalc: true, scan: true, scanChat: false, alertSound: false } });
  }

  if (req.method === 'GET' && url.pathname === '/prices') {
    const since = Number(url.searchParams.get('since')) || 0;
    const items = [];
    for (const k in db) {
      const it = db[k];
      if (it.lastSeen > since) {
        items.push({ name: it.name, source: it.source, category: it.category, lowestBin: it.lowestBin,
                     sellPrice: it.sellPrice, avgPrice: it.avgPrice, lastSeen: it.lastSeen });
      }
    }
    return send(res, 200, { now: Date.now(), count: Object.keys(db).length, items });
  }

  if (req.method === 'POST' && url.pathname === '/submit') {
    let size = 0; const chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > MAX_BODY) { send(res, 413, { error: 'too large' }); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      if (res.writableEnded) return;
      let arr;
      try { arr = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch (e) { return send(res, 400, { error: 'bad json' }); }
      if (!Array.isArray(arr)) return send(res, 400, { error: 'expected array' });
      const now = Date.now();
      let ok = 0;
      const tot = Math.min(arr.length, MAX_ITEMS_PER_POST);
      for (const e of arr.slice(0, MAX_ITEMS_PER_POST)) if (accept(e, now)) ok++;
      if (ok) dirty = true;
      stats.posts++; stats.accepted += ok; stats.rejected += tot - ok; if (ok) stats.lastUpload = now;
      const who = crypto.createHash('sha256').update(ip).digest('hex').slice(0, 8);
      uploaders.set(who, now);
      recent.unshift({ t: now, who, sent: tot, ok }); if (recent.length > 40) recent.pop();
      send(res, 200, { ok: true, accepted: ok });
    });
    return;
  }

  if (url.pathname === '/api/config' || url.pathname === '/api/login/google' || url.pathname.startsWith('/api/me')) return userApi(req, res, url);
  if (url.pathname === '/admin' && req.method === 'GET') {
    try { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' }); return res.end(fs.readFileSync(path.join(__dirname, 'admin.html'))); }
    catch (e) { return send(res, 404, 'admin.html missing', 'text/plain'); }
  }
  if (url.pathname.startsWith('/admin/api/')) return adminApi(req, res, url, ip);

  send(res, 404, { error: 'not found' });
});

// keep the free Render server awake (Render sets RENDER_EXTERNAL_URL itself)
if (process.env.RENDER_EXTERNAL_URL) {
  setInterval(() => {
    http.get(process.env.RENDER_EXTERNAL_URL + '/health', r => r.resume()).on('error', () => {});
  }, 10 * 60 * 1000).unref();
}

loadLocal();
server.listen(PORT, () => console.log('Price Tracker server on port ' + PORT));   // port opens at once so Render sees the service as healthy
loadRemote().finally(() => {
  const cleaned = purgeBad(); if (cleaned) console.log('Removed ' + cleaned + ' menu-button / bad items');
  console.log('Loaded ' + Object.keys(db).length + ' items');
});

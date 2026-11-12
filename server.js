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
const path = require('path');

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

function load() {
  try { db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) || {}; }
  catch (e) { db = {}; }
  console.log('Loaded ' + Object.keys(db).length + ' items');
}
function save() {
  if (!dirty) return;
  try {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DATA_FILE);
    dirty = false;
  } catch (e) { console.error('save failed', e.message); }
}
setInterval(save, 30000).unref();
process.on('SIGTERM', () => { save(); process.exit(0); });
process.on('SIGINT', () => { save(); process.exit(0); });

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
function accept(e, now) {
  if (!e || typeof e !== 'object') return false;
  let name = typeof e.name === 'string' ? e.name.trim() : '';
  if (name.length < 2 || name.length > 80 || /[\u0000-\u001f<>]/.test(name)) return false;
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
      for (const e of arr.slice(0, MAX_ITEMS_PER_POST)) if (accept(e, now)) ok++;
      if (ok) dirty = true;
      send(res, 200, { ok: true, accepted: ok });
    });
    return;
  }

  send(res, 404, { error: 'not found' });
});

// keep the free Render server awake (Render sets RENDER_EXTERNAL_URL itself)
if (process.env.RENDER_EXTERNAL_URL) {
  setInterval(() => {
    http.get(process.env.RENDER_EXTERNAL_URL + '/health', r => r.resume()).on('error', () => {});
  }, 10 * 60 * 1000).unref();
}

load();
server.listen(PORT, () => console.log('Price Tracker server on port ' + PORT));

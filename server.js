/**
 * FlowByMk - Real-Time Backend Cookie Monitor Panel
 * Runs on http://localhost:3300
 * Polls backend cookies every 0.5s (500ms) with zero latency
 * Tracks exact timestamps of updates, changes, and provides full inspection & copy tools.
 */

const http = require('http');
const https = require('https');
const url = require('url');
const crypto = require('crypto');

const PORT = process.env.PORT || 3300;

// Configuration state
let config = {
  backendUrl: 'https://portal.aqmart.tech',
  token: '',
  slot: '1',
  pollIntervalMs: 500,
  autoPoll: true
};

// Monitor state
let monitorState = {
  status: 'idle', // 'idle' | 'polling' | 'connected' | 'error'
  lastPollTime: null,
  lastUpdateTime: null,
  lastChangeTime: null,
  updateCount: 0,
  lastHash: '',
  cookieCount: 0,
  cookies: [],
  rawPayload: null,
  slots: [],
  errorMessage: '',
  history: []
};

function addHistory(type, message, details = '') {
  const now = new Date();
  const timeStr = now.toLocaleTimeString('en-US', { hour12: true, hour: '2-digit', minute: '2-digit', second: '2-digit' }) + 
    '.' + String(now.getMilliseconds()).padStart(3, '0');
  monitorState.history.unshift({
    id: Date.now() + Math.random(),
    time: timeStr,
    timestamp: now.toISOString(),
    type,
    message,
    details
  });
  if (monitorState.history.length > 50) monitorState.history.pop();
}

// Fetch helper using native node https/http
function fetchBackend(targetUrl, headers = {}) {
  return new Promise((resolve, reject) => {
    try {
      const parsed = new URL(targetUrl);
      const isHttps = parsed.protocol === 'https:';
      const client = isHttps ? https : http;

      const reqHeaders = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'FlowByMK-Cookie-Monitor/1.0',
        ...headers
      };

      if (config.token) {
        reqHeaders['X-Session-ID'] = config.token;
        reqHeaders['Authorization'] = `Bearer ${config.token}`;
      }

      const req = client.request(parsed, {
        method: 'GET',
        headers: reqHeaders,
        timeout: 4000
      }, res => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            const parsedJson = JSON.parse(data);
            resolve({ status: res.statusCode, data: parsedJson });
          } catch (_) {
            resolve({ status: res.statusCode, data: data });
          }
        });
      });

      req.on('error', err => reject(err));
      req.on('timeout', () => { req.destroy(); reject(new Error('Backend request timed out (4s)')); });
      req.end();
    } catch (err) {
      reject(err);
    }
  });
}

// Fetch slots list
async function fetchSlots() {
  const base = config.backendUrl.replace(/\/+$/, '');
  const endpoints = ['/api/extension/session.php', '/api/user/cookie-sessions', '/api/cookies'];
  for (const ep of endpoints) {
    try {
      const res = await fetchBackend(base + ep);
      if (res.status === 200 && res.data) {
        const raw = res.data.slots || res.data.sessions || res.data.files || [];
        if (Array.isArray(raw) && raw.length > 0) {
          monitorState.slots = raw.map((s, idx) => ({
            key: String(s.id != null ? s.id : (s.slot_key != null ? s.slot_key : idx + 1)),
            name: s.name || `Session ${idx + 1}`,
            cookieCount: s.cookie_count || s.cookieCount || 0
          }));
          return;
        }
      }
    } catch (_) {}
  }
}

// Fetch cookies for currently selected slot
async function pollCookies() {
  monitorState.lastPollTime = new Date().toISOString();
  const base = config.backendUrl.replace(/\/+$/, '');
  const slot = config.slot;

  const endpoints = [
    `/api/extension/cookies.php?slot=${encodeURIComponent(slot)}`,
    `/api/cookies/${encodeURIComponent(slot)}`,
    `/api/cookies?slot=${encodeURIComponent(slot)}`
  ];

  let lastErr = null;
  for (const ep of endpoints) {
    try {
      const res = await fetchBackend(base + ep);
      if (res.status >= 200 && res.status < 300 && res.data) {
        handleCookiePayload(res.data);
        monitorState.status = 'connected';
        monitorState.errorMessage = '';
        return;
      } else if (res.status === 401 || res.status === 403) {
        monitorState.status = 'error';
        monitorState.errorMessage = 'Authentication required. Please enter your Token / Session ID.';
        return;
      }
    } catch (err) {
      lastErr = err;
    }
  }

  if (lastErr) {
    monitorState.status = 'error';
    monitorState.errorMessage = lastErr.message || 'Could not connect to backend';
  }
}

function handleCookiePayload(payload) {
  let rawCookies = payload?.cookies || payload?.data || payload;
  if (typeof rawCookies === 'string') {
    try { rawCookies = JSON.parse(rawCookies); } catch (_) {}
  }

  let cookieArray = [];
  if (Array.isArray(rawCookies)) {
    cookieArray = rawCookies;
  } else if (typeof rawCookies === 'object' && rawCookies !== null) {
    cookieArray = Object.entries(rawCookies).map(([name, value]) => ({ name, value }));
  }

  const payloadString = typeof rawCookies === 'string' ? rawCookies : JSON.stringify(rawCookies);
  const hash = crypto.createHash('md5').update(payloadString).digest('hex');

  const now = new Date();
  const timeFormatted = now.toLocaleTimeString('en-US', { hour12: true, hour: '2-digit', minute: '2-digit', second: '2-digit' });

  if (monitorState.lastHash !== hash) {
    // New or updated cookie payload!
    const isFirst = !monitorState.lastHash;
    monitorState.lastHash = hash;
    monitorState.lastUpdateTime = now.toISOString();
    monitorState.lastChangeTime = timeFormatted;
    monitorState.updateCount++;
    monitorState.cookieCount = cookieArray.length;
    monitorState.cookies = cookieArray;
    monitorState.rawPayload = rawCookies;

    addHistory(
      isFirst ? 'init' : 'update',
      isFirst ? `Initial cookies loaded (${cookieArray.length} cookies)` : `Cookies updated from backend! (${cookieArray.length} cookies)`,
      `Hash: ${hash.slice(0, 8)}... | Slot: ${config.slot}`
    );
  }
}

// Background poller loop
let pollTimer = null;
function startPoller() {
  if (pollTimer) clearTimeout(pollTimer);
  const run = async () => {
    if (config.autoPoll) {
      try { await pollCookies(); } catch (_) {}
    }
    pollTimer = setTimeout(run, Math.max(200, config.pollIntervalMs));
  };
  run();
}

// Initialize slots periodically
setInterval(fetchSlots, 10000);
fetchSlots();
startPoller();

// HTML Dashboard Template
const HTML_PAGE = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>FlowByMK — Live Backend Cookie Monitor</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0d0e12;
      --card-bg: #15171e;
      --card-border: rgba(255, 255, 255, 0.08);
      --accent: #e11d34;
      --accent-glow: rgba(225, 29, 52, 0.35);
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.35);
      --yellow: #f59e0b;
      --text: #f3f4f6;
      --text-muted: #9ca3af;
      --code-bg: #090a0d;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Plus Jakarta Sans', sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      padding: 24px;
    }
    .container { max-width: 1400px; margin: 0 auto; width: 100%; display: flex; flex-direction: column; gap: 20px; }
    
    /* Top Bar */
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
      padding-bottom: 8px;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand-icon {
      width: 42px; height: 42px; border-radius: 12px;
      background: linear-gradient(135deg, #e11d34, #ff5a4f);
      display: flex; align-items: center; justify-content: center;
      font-weight: 800; font-size: 20px; color: #fff;
      box-shadow: 0 0 20px var(--accent-glow);
    }
    .brand h1 { font-size: 22px; font-weight: 800; letter-spacing: -0.5px; }
    .brand h1 span { color: var(--accent); }
    .brand p { font-size: 13px; color: var(--text-muted); }
    
    .status-badge {
      display: flex; align-items: center; gap: 8px;
      padding: 8px 16px; border-radius: 999px;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      font-size: 13px; font-weight: 600; color: #34d399;
    }
    .status-badge.error { background: rgba(225, 29, 52, 0.12); border-color: rgba(225, 29, 52, 0.3); color: #f87171; }
    .pulse-dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #10b981; box-shadow: 0 0 12px #10b981;
      animation: pulse 1.5s infinite;
    }
    .status-badge.error .pulse-dot { background: #e11d34; box-shadow: 0 0 12px #e11d34; animation: none; }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }

    /* Control Panel */
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 18px;
      padding: 20px;
    }
    .config-grid {
      display: grid;
      grid-template-columns: 2fr 1.5fr 1fr 1fr auto;
      gap: 12px;
      align-items: flex-end;
    }
    @media (max-width: 900px) { .config-grid { grid-template-columns: 1fr; } }
    .form-group { display: flex; flex-direction: column; gap: 6px; }
    .form-group label { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted); }
    input, select {
      background: var(--code-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 10px 14px;
      color: #fff;
      font-size: 14px;
      outline: none;
      transition: border-color 0.2s;
    }
    input:focus, select:focus { border-color: var(--accent); }
    .btn {
      padding: 10px 18px;
      border-radius: 10px;
      border: 0;
      font-size: 13.5px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      transition: transform 0.15s, filter 0.15s;
    }
    .btn:hover { transform: translateY(-1px); filter: brightness(1.1); }
    .btn.primary { background: linear-gradient(135deg, #e11d34, #ff5a4f); color: #fff; box-shadow: 0 4px 15px var(--accent-glow); }
    .btn.secondary { background: rgba(255,255,255,0.08); color: #fff; border: 1px solid rgba(255,255,255,0.12); }
    .btn.toggle.active { background: rgba(16,185,129,0.18); border-color: rgba(16,185,129,0.4); color: #34d399; }

    /* Stats Grid */
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
    }
    @media (max-width: 800px) { .stats-grid { grid-template-columns: repeat(2, 1fr); } }
    .stat-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      padding: 18px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .stat-label { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted); }
    .stat-value { font-size: 26px; font-weight: 800; font-family: 'JetBrains Mono', monospace; }
    .stat-value.highlight { color: #34d399; }
    .stat-sub { font-size: 12px; color: var(--text-muted); }

    /* Main layout */
    .main-grid {
      display: grid;
      grid-template-columns: 1fr 340px;
      gap: 20px;
    }
    @media (max-width: 1050px) { .main-grid { grid-template-columns: 1fr; } }

    /* Section Tabs */
    .tabs-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
    }
    .tabs { display: flex; gap: 8px; }
    .tab-btn {
      padding: 8px 16px; border-radius: 8px; border: 0;
      background: transparent; color: var(--text-muted);
      font-size: 13.5px; font-weight: 600; cursor: pointer;
    }
    .tab-btn.active { background: rgba(255,255,255,0.08); color: #fff; }

    /* Table */
    .table-container {
      overflow-x: auto;
      border-radius: 12px;
      border: 1px solid var(--card-border);
      background: var(--code-bg);
    }
    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; }
    th {
      padding: 12px 16px;
      background: rgba(255,255,255,0.03);
      border-bottom: 1px solid var(--card-border);
      color: var(--text-muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    td {
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      font-family: 'JetBrains Mono', monospace;
    }
    tr:hover td { background: rgba(255,255,255,0.02); }
    .cookie-name { font-weight: 700; color: #60a5fa; }
    .cookie-name.key-cookie { color: #f43f5e; }
    .cookie-val {
      max-width: 380px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #94a3b8;
    }

    /* Raw JSON view */
    .json-view {
      background: var(--code-bg);
      border-radius: 12px;
      border: 1px solid var(--card-border);
      padding: 16px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 12.5px;
      color: #cbd5e1;
      max-height: 480px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }

    /* History Feed */
    .history-card { display: flex; flex-direction: column; gap: 12px; height: 100%; }
    .history-list {
      display: flex; flex-direction: column; gap: 8px;
      max-height: 520px; overflow-y: auto; padding-right: 4px;
    }
    .history-item {
      padding: 12px; border-radius: 10px;
      background: var(--code-bg);
      border-left: 3px solid var(--card-border);
      font-size: 12px; display: flex; flex-direction: column; gap: 4px;
    }
    .history-item.update { border-left-color: #10b981; }
    .history-item.init { border-left-color: #3b82f6; }
    .history-time { font-family: 'JetBrains Mono', monospace; color: var(--text-muted); font-size: 11px; }
    .history-msg { font-weight: 600; color: #fff; }
    .history-detail { color: #64748b; font-family: 'JetBrains Mono', monospace; font-size: 10.5px; }

    /* Toast */
    .toast {
      position: fixed; bottom: 24px; right: 24px;
      padding: 12px 20px; border-radius: 12px;
      background: #10b981; color: #fff;
      font-weight: 600; font-size: 14px;
      box-shadow: 0 10px 25px rgba(0,0,0,0.5);
      transform: translateY(100px); opacity: 0;
      transition: transform 0.25s, opacity 0.25s;
      z-index: 999;
    }
    .toast.show { transform: translateY(0); opacity: 1; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <div class="brand-icon">MK</div>
        <div>
          <h1>FlowBy<span>Mk</span> Live Cookie Monitor</h1>
          <p>Real-time backend sync & automatic update detection</p>
        </div>
      </div>
      <div id="statusBadge" class="status-badge">
        <div class="pulse-dot"></div>
        <span id="statusText">CONNECTING...</span>
      </div>
    </header>

    <!-- Controls -->
    <div class="card">
      <div class="config-grid">
        <div class="form-group">
          <label>Backend URL</label>
          <input type="text" id="backendInput" value="https://portal.aqmart.tech" placeholder="https://...">
        </div>
        <div class="form-group">
          <label>Auth Token / Session ID (Optional)</label>
          <input type="password" id="tokenInput" placeholder="Leave empty or enter Token">
        </div>
        <div class="form-group">
          <label>Active Slot</label>
          <select id="slotSelect">
            <option value="1">Slot 1</option>
          </select>
        </div>
        <div class="form-group">
          <label>Poll Speed</label>
          <select id="intervalSelect">
            <option value="500" selected>0.5s (500ms)</option>
            <option value="1000">1.0s</option>
            <option value="2000">2.0s</option>
          </select>
        </div>
        <div style="display: flex; gap: 8px;">
          <button class="btn primary" id="btnApply">Save</button>
          <button class="btn secondary" id="btnFetchNow">Fetch Now</button>
        </div>
      </div>
    </div>

    <!-- Stats -->
    <div class="stats-grid">
      <div class="stat-card">
        <span class="stat-label">Last Updated Time</span>
        <span class="stat-value highlight" id="lastUpdateTime">—</span>
        <span class="stat-sub" id="lastUpdatedRelative">Waiting for update</span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Total Cookies</span>
        <span class="stat-value" id="cookieCount">0</span>
        <span class="stat-sub" id="hashSub">Hash: —</span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Total Updates Detected</span>
        <span class="stat-value" id="updateCount">0</span>
        <span class="stat-sub">Live streaming active</span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Polling Rate</span>
        <span class="stat-value" id="pollRateDisplay">500ms</span>
        <span class="stat-sub" id="lastPollSub">Last poll: just now</span>
      </div>
    </div>

    <!-- Main Content -->
    <div class="main-grid">
      <div class="card">
        <div class="tabs-header">
          <div class="tabs">
            <button class="tab-btn active" id="tabTable">Cookies Table</button>
            <button class="tab-btn" id="tabJson">Raw JSON</button>
            <button class="tab-btn" id="tabHeader">Cookie Header</button>
          </div>
          <div style="display: flex; gap: 8px;">
            <input type="text" id="searchCookies" placeholder="Filter cookies..." style="padding: 6px 12px; font-size: 12px; width: 180px;">
            <button class="btn secondary" id="btnCopyJson" style="padding: 6px 12px; font-size: 12px;">Copy JSON</button>
            <button class="btn secondary" id="btnCopyHeader" style="padding: 6px 12px; font-size: 12px;">Copy Header</button>
          </div>
        </div>

        <div id="viewTable" class="table-container">
          <table>
            <thead>
              <tr>
                <th>Cookie Name</th>
                <th>Value</th>
                <th>Domain</th>
                <th>Path</th>
              </tr>
            </thead>
            <tbody id="cookieTableBody">
              <tr><td colspan="4" style="text-align:center; color:#64748b; padding: 24px;">Waiting for cookies...</td></tr>
            </tbody>
          </table>
        </div>

        <div id="viewJson" class="json-view" style="display: none;"></div>
        <div id="viewHeader" class="json-view" style="display: none;"></div>
      </div>

      <!-- History Log -->
      <div class="card history-card">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <h3 style="font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted);">Live Update History</h3>
          <span style="font-size: 11px; color: #10b981; font-weight: 600;">REAL-TIME</span>
        </div>
        <div class="history-list" id="historyList">
          <div style="color: #64748b; font-size: 12px; text-align: center; padding: 20px;">No updates recorded yet.</div>
        </div>
      </div>
    </div>
  </div>

  <div id="toast" class="toast">Copied to clipboard!</div>

  <script>
    const $ = id => document.getElementById(id);
    let currentCookies = [];
    let currentRaw = null;
    let activeTab = 'table';

    function showToast(msg) {
      const t = $('toast');
      t.textContent = msg;
      t.classList.add('show');
      setTimeout(() => t.classList.remove('show'), 2200);
    }

    // Tab switching
    $('tabTable').onclick = () => switchTab('table');
    $('tabJson').onclick = () => switchTab('json');
    $('tabHeader').onclick = () => switchTab('header');

    function switchTab(tab) {
      activeTab = tab;
      $('tabTable').classList.toggle('active', tab === 'table');
      $('tabJson').classList.toggle('active', tab === 'json');
      $('tabHeader').classList.toggle('active', tab === 'header');

      $('viewTable').style.display = tab === 'table' ? 'block' : 'none';
      $('viewJson').style.display = tab === 'json' ? 'block' : 'none';
      $('viewHeader').style.display = tab === 'header' ? 'block' : 'none';
    }

    // Copying
    $('btnCopyJson').onclick = () => {
      navigator.clipboard.writeText(JSON.stringify(currentRaw || currentCookies, null, 2));
      showToast('Copied JSON cookies!');
    };
    $('btnCopyHeader').onclick = () => {
      const header = currentCookies.map(c => \`\${c.name}=\${c.value}\`).join('; ');
      navigator.clipboard.writeText(header);
      showToast('Copied Cookie Header string!');
    };

    // Apply config
    $('btnApply').onclick = async () => {
      const payload = {
        backendUrl: $('backendInput').value.trim(),
        token: $('tokenInput').value.trim(),
        slot: $('slotSelect').value,
        pollIntervalMs: parseInt($('intervalSelect').value, 10)
      };
      await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      showToast('Configuration updated!');
      fetchStatus();
    };

    $('btnFetchNow').onclick = () => fetchStatus(true);
    $('searchCookies').oninput = renderTable;

    function renderTable() {
      const q = $('searchCookies').value.toLowerCase().trim();
      const filtered = currentCookies.filter(c => 
        (c.name || '').toLowerCase().includes(q) || (c.value || '').toLowerCase().includes(q)
      );

      if (!filtered.length) {
        $('cookieTableBody').innerHTML = '<tr><td colspan="4" style="text-align:center; color:#64748b; padding: 24px;">No cookies matching filter</td></tr>';
        return;
      }

      const keyRegex = /^(?:__Secure-[13]PSID|HSID|SSID|SID|APISID|SAPISID)$/i;

      $('cookieTableBody').innerHTML = filtered.map(c => {
        const isKey = keyRegex.test(c.name || '');
        return \`<tr>
          <td class="cookie-name \${isKey ? 'key-cookie' : ''}">\${c.name || ''} \${isKey ? '⭐' : ''}</td>
          <td class="cookie-val" title="\${c.value || ''}">\${c.value || ''}</td>
          <td style="color:#94a3b8">\${c.domain || 'google.com'}</td>
          <td style="color:#64748b">\${c.path || '/'}</td>
        </tr>\`;
      }).join('');
    }

    // Poll status from our local server
    async function fetchStatus(manual = false) {
      try {
        const res = await fetch('/api/status' + (manual ? '?pollNow=1' : ''));
        const data = await res.json();
        
        // Update header & badges
        const badge = $('statusBadge');
        if (data.status === 'connected') {
          badge.className = 'status-badge';
          $('statusText').textContent = 'LIVE · ' + (data.config.pollIntervalMs) + 'ms';
        } else if (data.status === 'error') {
          badge.className = 'status-badge error';
          $('statusText').textContent = data.errorMessage || 'ERROR';
        }

        // Stats
        $('lastUpdateTime').textContent = data.lastChangeTime || '—';
        $('cookieCount').textContent = data.cookieCount || 0;
        $('updateCount').textContent = data.updateCount || 0;
        $('pollRateDisplay').textContent = (data.config.pollIntervalMs) + 'ms';
        $('hashSub').textContent = data.lastHash ? 'MD5: ' + data.lastHash.slice(0, 10) : 'Hash: —';

        if (data.lastUpdateTime) {
          const sec = Math.round((Date.now() - new Date(data.lastUpdateTime).getTime()) / 1000);
          $('lastUpdatedRelative').textContent = sec < 2 ? 'Updated just now' : \`Updated \${sec}s ago\`;
        }

        currentCookies = data.cookies || [];
        currentRaw = data.rawPayload || currentCookies;

        // Render views
        renderTable();
        $('viewJson').textContent = JSON.stringify(currentRaw, null, 2);
        $('viewHeader').textContent = currentCookies.map(c => \`\${c.name}=\${c.value}\`).join('; ');

        // History
        if (data.history && data.history.length) {
          $('historyList').innerHTML = data.history.map(h => \`
            <div class="history-item \${h.type}">
              <div class="history-time">\${h.time}</div>
              <div class="history-msg">\${h.message}</div>
              \${h.details ? \`<div class="history-detail">\${h.details}</div>\` : ''}
            </div>
          \`).join('');
        }

        // Populate slots if changed
        if (data.slots && data.slots.length && $('slotSelect').options.length <= 1) {
          $('slotSelect').innerHTML = data.slots.map(s => 
            \`<option value="\${s.key}" \${s.key === data.config.slot ? 'selected' : ''}>\${s.name} (\${s.cookieCount || 0} cookies)</option>\`
          ).join('');
        }
      } catch (e) {
        $('statusBadge').className = 'status-badge error';
        $('statusText').textContent = 'SERVER DISCONNECTED';
      }
    }

    // Refresh browser UI every 500ms
    setInterval(fetchStatus, 500);
    fetchStatus();
  </script>
</body>
</html>`;

// Create HTTP server
const server = http.createServer(async (req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // CORS headers for local access
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (pathname === '/' || pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(HTML_PAGE);
    return;
  }

  if (pathname === '/api/status') {
    if (parsedUrl.query.pollNow === '1') {
      try { await pollCookies(); } catch (_) {}
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ...monitorState, config }));
    return;
  }

  if (pathname === '/api/config' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const patch = JSON.parse(body);
        if (patch.backendUrl) config.backendUrl = patch.backendUrl;
        if (patch.token !== undefined) config.token = patch.token;
        if (patch.slot) config.slot = patch.slot;
        if (patch.pollIntervalMs) config.pollIntervalMs = parseInt(patch.pollIntervalMs, 10);
        await fetchSlots();
        await pollCookies();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, config }));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

server.listen(PORT, () => {
  console.log(`[FlowByMK] Live Cookie Monitor running on http://localhost:${PORT}`);
});

// src/tunnel-do.js — Durable Object giữ kết nối tunnel

export class TunnelDO {
  constructor(state, env) {
    this.state = state;
    this.env   = env;

    this.tunnel  = null;        // WebSocket của client A (tunnel agent)
    this.pending = new Map();   // id → {resolve, reject}  (HTTP + ws-accept)
    this.clients = new Map();   // id → WebSocket của client B (ws proxy)
  }

	const TUNNEL_TOKEN = 'abc'

  // ─── Router ────────────────────────────────────────────────────────────────

  async fetch(request) {
    const url     = new URL(request.url);
    const upgrade = request.headers.get('Upgrade');

    // Client A kết nối tunnel
    if (url.pathname === '/$web_tunnel') {
      return this._acceptTunnel(request);
    }

    if (!this.tunnel || this.tunnel.readyState !== WebSocket.OPEN) {
      return new Response('No tunnel connected', { status: 502 });
    }

    // Client B — WebSocket
    if (upgrade === 'websocket') {
      return this._proxyWs(request);
    }

    // Client B — HTTP
    return this._proxyHttp(request);
  }

  // ─── Tunnel (client A) ─────────────────────────────────────────────────────

  _acceptTunnel(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('WebSocket required', { status: 426 });
    }

    // Xác thực token đơn giản
    const token = request.headers.get('x-tunnel-token')
                  || new URL(request.url).searchParams.get('token');
    if (TUNNEL_TOKEN && token !== TUNNEL_TOKEN) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Đóng tunnel cũ nếu có
    if (this.tunnel) {
      try { this.tunnel.close(1000, 'Replaced by new connection'); } catch {}
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    this.tunnel = server;

    server.addEventListener('message', (e) => this._onTunnelMsg(e.data));
    server.addEventListener('close',   ()  => this._onTunnelClose());
    server.addEventListener('error',   ()  => this._onTunnelClose());

    console.log('Tunnel client A connected');
    return new Response(null, { status: 101, webSocket: client });
  }

  _onTunnelClose() {
    this.tunnel = null;
    // Báo lỗi tất cả request đang chờ
    for (const [, { reject }] of this.pending) {
      reject(new Error('Tunnel disconnected'));
    }
    this.pending.clear();
    // Đóng tất cả WS của client B
    for (const [, ws] of this.clients) {
      try { ws.close(1001, 'Tunnel disconnected'); } catch {}
    }
    this.clients.clear();
  }

  _onTunnelMsg(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const { type, id } = msg;

    // Trả lời HTTP hoặc ws-accept/ws-reject
    const p = this.pending.get(id);
    if (p && (type === 'response' || type === 'ws-accept' || type === 'ws-reject')) {
      this.pending.delete(id);
      p.resolve(msg);
      return;
    }

    // Dữ liệu / đóng WS cho client B
    if (type === 'ws-data') {
      const ws = this.clients.get(id);
      if (!ws) return;
      const payload = msg.binary ? b64ToBuffer(msg.data) : msg.data;
      ws.send(payload);
    } else if (type === 'ws-close') {
      const ws = this.clients.get(id);
      if (!ws) return;
      this.clients.delete(id);
      try { ws.close(1000, 'Closed by tunnel'); } catch {}
    }
  }

  // ─── HTTP proxy (client B → A) ─────────────────────────────────────────────

  async _proxyHttp(request) {
    const id   = crypto.randomUUID();
    const body = await request.arrayBuffer();

    this.tunnel.send(JSON.stringify({
      type:    'request',
      id,
      method:  request.method,
      url:     request.url,
      headers: headersToObj(request.headers),
      body:    body.byteLength ? bufToB64(body) : null,
    }));

    let msg;
    try {
      msg = await this._waitFor(id, 30_000);
    } catch (e) {
      return new Response(e.message, { status: 502 });
    }

    const resBody = msg.body ? b64ToBuffer(msg.body) : null;
    return new Response(resBody, {
      status:  msg.status  || 200,
      headers: msg.headers || {},
    });
  }

  // ─── WebSocket proxy (client B ↔ A) ────────────────────────────────────────

  async _proxyWs(request) {
    const id = crypto.randomUUID();

    // Báo A có WS mới
    this.tunnel.send(JSON.stringify({
      type:    'ws-connect',
      id,
      url:     request.url,
      headers: headersToObj(request.headers),
    }));

    let reply;
    try {
      reply = await this._waitFor(id, 10_000);
    } catch (e) {
      return new Response('Tunnel WS accept timeout', { status: 502 });
    }

    if (reply.type !== 'ws-accept') {
      return new Response('Tunnel rejected WS', { status: 502 });
    }

    // Nâng cấp kết nối với B
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    this.clients.set(id, server);

    server.addEventListener('message', (e) => {
      if (!this.tunnel) return;
      const binary = e.data instanceof ArrayBuffer;
      this.tunnel.send(JSON.stringify({
        type:   'ws-data',
        id,
        binary,
        data:   binary ? bufToB64(e.data) : e.data,
      }));
    });

    server.addEventListener('close', () => {
      this.clients.delete(id);
      if (this.tunnel) {
        try { this.tunnel.send(JSON.stringify({ type: 'ws-close', id })); } catch {}
      }
    });

    return new Response(null, {
      status:    101,
      webSocket: client,
      headers:   reply.headers || {},
    });
  }

  // ─── Helpers ───────────────────────────────────────────────────────────────

  _waitFor(id, ms) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timeout waiting for id=${id}`));
      }, ms);
      this.pending.set(id, {
        resolve: (v) => { clearTimeout(timer); resolve(v); },
        reject:  (e) => { clearTimeout(timer); reject(e); },
      });
    });
  }
}

// ─── Util ──────────────────────────────────────────────────────────────────

function headersToObj(headers) {
  const obj = {};
  for (const [k, v] of headers) obj[k] = v;
  return obj;
}

function bufToB64(buf) {
  const bytes = new Uint8Array(buf instanceof ArrayBuffer ? buf : buf);
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
}

function b64ToBuffer(b64) {
  const s  = atob(b64);
  const ab = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) ab[i] = s.charCodeAt(i);
  return ab.buffer;
}

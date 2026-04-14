// src/tunnel-do.js — Durable Object giữ kết nối tunnel
// Deploy lên Cloudflare Workers
// Sử dụng Cloudflare WebSocket Hibernation API

// ─── Binary Frame Protocol ────────────────────────────────────────────────────
const HEADER    = 41;
const CHUNK_SZ  = 256 * 1024;

// PING_MS = 10 giây (giảm từ 20s):
// - CF đóng WS nếu không có frame trong 100s  → 10s PING cho buffer rộng
// - Nhiều NAT/router drop idle TCP sau 30s      → 10s PING giữ TCP alive
// - Alarm có thể trễ vài giây                  → margin an toàn hơn
const PING_MS   = 10_000;

const ZERO_ID = '00000000-0000-0000-0000-000000000000';

const TAG_TUNNEL = 'tunnel';
const tagClient  = id => `client:${id}`;

const M = Object.freeze({
  HTTP_REQ_HEAD:  0x01,
  HTTP_REQ_CHUNK: 0x02,
  HTTP_REQ_END:   0x03,
  HTTP_RES_HEAD:  0x04,
  HTTP_RES_CHUNK: 0x05,
  HTTP_RES_END:   0x06,
  WS_CONNECT:     0x07,
  WS_ACCEPT:      0x08,
  WS_REJECT:      0x09,
  WS_DATA:        0x0A,
  WS_CLOSE:       0x0B,
  PING:           0x0C,
  PONG:           0x0D,
});

const _te = new TextEncoder();
const _td = new TextDecoder();

function encode(type, id, meta, payload) {
  const mBuf = meta    ? _te.encode(JSON.stringify(meta)) : new Uint8Array(0);
  const pBuf = payload instanceof Uint8Array ? payload
             : payload instanceof ArrayBuffer ? new Uint8Array(payload)
             : payload                        ? new Uint8Array(payload)
             :                                   new Uint8Array(0);
  const out = new Uint8Array(HEADER + mBuf.length + pBuf.length);
  const dv  = new DataView(out.buffer);
  out[0] = type;
  out.set(_te.encode((id || ZERO_ID).slice(0, 36).padEnd(36, '0')), 1);
  dv.setUint32(37, mBuf.length, false);
  out.set(mBuf, HEADER);
  out.set(pBuf, HEADER + mBuf.length);
  return out.buffer;
}

function decode(raw) {
  let u8;
  if (raw instanceof ArrayBuffer) {
    u8 = new Uint8Array(raw);
  } else if (ArrayBuffer.isView(raw)) {
    u8 = new Uint8Array(raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength));
  } else {
    u8 = new Uint8Array(raw);
  }
  const dv = new DataView(u8.buffer);
  if (u8.length < HEADER) throw new Error('frame quá ngắn');
  const type    = u8[0];
  const id      = _td.decode(u8.subarray(1, 37));
  const metaLen = dv.getUint32(37, false);
  const meta    = metaLen
    ? JSON.parse(_td.decode(u8.subarray(HEADER, HEADER + metaLen)))
    : null;
  const payload = u8.slice(HEADER + metaLen);
  return { type, id, meta, payload };
}

// ─── Durable Object ───────────────────────────────────────────────────────────

export class TunnelDO {
  constructor(state, env) {
    this.state   = state;
    this.env     = env;
    this.pending = new Map();
    this.clients = new Map(); // cache id → WebSocket (client B)
  }

  // ─── Router ────────────────────────────────────────────────────────────────

  async fetch(request) {
    const TUNNEL_TOKEN = 'abc';
    const url     = new URL(request.url);
    const upgrade = request.headers.get('Upgrade');

    if (url.pathname.includes('/reset')) {
      this._closeTunnel('Reset requested');
      return new Response('ok', { status: 200 });
    }

    if (url.pathname === '/$web_tunnel') {
      return this._acceptTunnel(request, TUNNEL_TOKEN);
    }

    if (!this._getTunnelWs()) {
      return new Response('No tunnel connected', { status: 200 });
    }

    if (upgrade === 'websocket') return this._proxyWs(request);
    return this._proxyHttp(request);
  }

  // ─── Helpers lấy WebSocket từ Hibernation runtime ─────────────────────────

  _getTunnelWs() {
    const sockets = this.state.getWebSockets(TAG_TUNNEL);
    return sockets.length > 0 ? sockets[0] : null;
  }

  _getClientWs(id) {
    if (this.clients.has(id)) return this.clients.get(id);
    const sockets = this.state.getWebSockets(tagClient(id));
    if (sockets.length > 0) {
      this.clients.set(id, sockets[0]);
      return sockets[0];
    }
    return null;
  }

  // FIX: Dùng this.state.getTags(ws) thay vì ws.tags
  // ws.tags KHÔNG tồn tại trong Cloudflare Hibernation API.
  // getTags() là method duy nhất để lấy tag của một WebSocket sau hibernate.
  _getWsTags(ws) {
    try {
      return this.state.getTags(ws) ?? [];
    } catch {
      return [];
    }
  }

  // ─── Chấp nhận tunnel từ client A ─────────────────────────────────────────

  _acceptTunnel(request, token) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('WebSocket required', { status: 426 });
    }

    const clientToken = request.headers.get('x-tunnel-token')
                     || new URL(request.url).searchParams.get('token');
    if (token && clientToken !== token) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Đóng tunnel cũ nếu có (client A reconnect)
    this._closeTunnel('Replaced by new connection');

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server, [TAG_TUNNEL]);
    this._scheduleNextPing();

    console.log('[DO] Tunnel client A kết nối');
    return new Response(null, { status: 101, webSocket: client });
  }

  _closeTunnel(reason) {
    this.state.storage.deleteAlarm().catch(() => {});
    const ws = this._getTunnelWs();
    if (ws) {
      try { ws.close(1000, reason); } catch {}
    }
  }

  _onTunnelClose() {
    this.state.storage.deleteAlarm().catch(() => {});
    console.log('[DO] Tunnel client A ngắt kết nối');

    for (const [, p] of this.pending) {
      if (p.writer) {
        p.writer.abort(new Error('Tunnel disconnected')).catch(() => {});
      } else {
        p.reject(new Error('Tunnel disconnected'));
      }
    }
    this.pending.clear();

    // Đóng tất cả WebSocket client B, bỏ qua tunnel
    for (const ws of this.state.getWebSockets()) {
      const tags = this._getWsTags(ws);
      if (tags.includes(TAG_TUNNEL)) continue;
      try { ws.close(1001, 'Tunnel disconnected'); } catch {}
    }
    this.clients.clear();
  }

  // ─── Hibernation API handlers ──────────────────────────────────────────────

  webSocketMessage(ws, message) {
    const tags = this._getWsTags(ws); // FIX: dùng getTags()

    if (tags.includes(TAG_TUNNEL)) {
      this._onTunnelMsg(message);
      return;
    }

    const clientTag = tags.find(t => t.startsWith('client:'));
    if (!clientTag) return;
    const id = clientTag.slice(7);

    const tunnel = this._getTunnelWs();
    if (!tunnel) return;

    const binary = message instanceof ArrayBuffer;
    this._tunnelSend(encode(M.WS_DATA, id, { binary },
      binary ? new Uint8Array(message) : _te.encode(message)));
  }

  webSocketClose(ws, code, reason, wasClean) {
    const tags = this._getWsTags(ws); // FIX: dùng getTags()

    if (tags.includes(TAG_TUNNEL)) {
      this._onTunnelClose();
      return;
    }

    const clientTag = tags.find(t => t.startsWith('client:'));
    if (!clientTag) return;
    const id = clientTag.slice(7);
    this.clients.delete(id);

    const tunnel = this._getTunnelWs();
    if (tunnel) {
      try { this._tunnelSend(encode(M.WS_CLOSE, id, null, null)); } catch {}
    }
  }

  webSocketError(ws, error) {
    const tags = this._getWsTags(ws); // FIX: dùng getTags()

    if (tags.includes(TAG_TUNNEL)) {
      // FIX: KHÔNG gọi ws.close() — WS đã lỗi, close() sẽ throw thêm lỗi
      console.error('[DO] Tunnel WebSocket lỗi:', error?.message ?? error);
      this._onTunnelClose();
      return;
    }

    const clientTag = tags.find(t => t.startsWith('client:'));
    if (!clientTag) return;
    const id = clientTag.slice(7);
    this.clients.delete(id);

    // FIX: không gọi ws.close() trên WS đã lỗi
    const tunnel = this._getTunnelWs();
    if (tunnel) {
      try { this._tunnelSend(encode(M.WS_CLOSE, id, null, null)); } catch {}
    }
  }

  // ─── Alarm — PING keepalive (Hibernation-safe, thay setInterval) ──────────

  _scheduleNextPing() {
    this.state.storage.setAlarm(Date.now() + PING_MS).catch(() => {});
  }

  async alarm() {
    const tunnel = this._getTunnelWs();
    if (!tunnel) return; // tunnel đã mất, dừng alarm

    try {
      tunnel.send(encode(M.PING, ZERO_ID, null, null));
    } catch {
      // send lỗi → tunnel đã chết
      this._onTunnelClose();
      return;
    }

    this._scheduleNextPing();
  }

  // ─── Dispatch message từ client A ─────────────────────────────────────────

  _onTunnelMsg(raw) {
    let f;
    try { f = decode(raw); } catch (e) {
      console.error('[DO] decode lỗi:', e.message);
      return;
    }

    switch (f.type) {

      case M.PING: {
        const tunnel = this._getTunnelWs();
        try { tunnel?.send(encode(M.PONG, ZERO_ID, null, null)); } catch {}
        break;
      }
      case M.PONG:
        break;

      case M.HTTP_RES_HEAD: {
        const p = this.pending.get(f.id);
        if (!p || p.writer) break;
        const { readable, writable } = new TransformStream();
        p.writer = writable.getWriter();
        p.resolve(new Response(readable, {
          status:  f.meta.status  || 200,
          headers: f.meta.headers || {},
        }));
        break;
      }

      case M.HTTP_RES_CHUNK: {
        const p = this.pending.get(f.id);
        if (p?.writer && f.payload.length > 0) {
          p.writer.write(f.payload).catch(() => {
            this.pending.delete(f.id);
          });
        }
        break;
      }

      case M.HTTP_RES_END: {
        const p = this.pending.get(f.id);
        this.pending.delete(f.id);
        if (!p) break;
        if (p.writer) {
          if (f.meta?.ok !== false) {
            p.writer.close().catch(() => {});
          } else {
            p.writer.abort(new Error(f.meta?.error || 'upstream error')).catch(() => {});
          }
        } else if (f.meta?.ok === false) {
          p.reject(new Error(f.meta?.error || 'upstream error'));
        }
        break;
      }

      case M.WS_ACCEPT:
      case M.WS_REJECT: {
        const p = this.pending.get(f.id);
        if (!p) break;
        this.pending.delete(f.id);
        p.resolve({ accepted: f.type === M.WS_ACCEPT });
        break;
      }

      case M.WS_DATA: {
        const ws = this._getClientWs(f.id);
        if (!ws) break;
        if (f.meta?.binary) {
          ws.send(f.payload.buffer.slice(
            f.payload.byteOffset,
            f.payload.byteOffset + f.payload.byteLength,
          ));
        } else {
          ws.send(_td.decode(f.payload));
        }
        break;
      }

      case M.WS_CLOSE: {
        const ws = this._getClientWs(f.id);
        this.clients.delete(f.id);
        if (ws) try { ws.close(1000, 'Closed by tunnel'); } catch {}
        break;
      }
    }
  }

  // ─── HTTP proxy ────────────────────────────────────────────────────────────

  async _proxyHttp(request) {
    const id = crypto.randomUUID();

    this._tunnelSend(encode(M.HTTP_REQ_HEAD, id, {
      method:  request.method,
      url:     request.url,
      headers: headersToObj(request.headers),
    }, null));

    if (request.body) {
      const reader = request.body.getReader();
      let bodyOk = true;
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          for (let off = 0; off < value.byteLength; off += CHUNK_SZ) {
            this._tunnelSend(encode(M.HTTP_REQ_CHUNK, id, null,
              value.subarray(off, Math.min(off + CHUNK_SZ, value.byteLength))));
          }
        }
      } catch {
        bodyOk = false;
      } finally {
        reader.releaseLock();
      }

      if (!bodyOk) {
        this._tunnelSend(encode(M.HTTP_REQ_END, id, { cancel: true }, null));
        return new Response('Request body read error', { status: 400 });
      }
    }

    this._tunnelSend(encode(M.HTTP_REQ_END, id, { cancel: false }, null));

    let response;
    try {
      response = await this._waitFor(id, 60_000);
    } catch (e) {
      return new Response(e.message, { status: 502 });
    }

    return response;
  }

  // ─── WebSocket proxy ───────────────────────────────────────────────────────

  async _proxyWs(request) {
    const id = crypto.randomUUID();

    this._tunnelSend(encode(M.WS_CONNECT, id, {
      url:     request.url,
      headers: headersToObj(request.headers),
    }, null));

    let reply;
    try {
      reply = await this._waitFor(id, 15_000);
    } catch {
      return new Response('WS accept timeout', { status: 502 });
    }

    if (!reply.accepted) {
      return new Response('Tunnel rejected WebSocket', { status: 502 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server, [tagClient(id)]);
    this.clients.set(id, server);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  _tunnelSend(frame) {
    const tunnel = this._getTunnelWs();
    if (tunnel) {
      try { tunnel.send(frame); } catch {}
    }
  }

  _waitFor(id, ms) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timeout id=${id}`));
      }, ms);
      this.pending.set(id, {
        resolve: v => { clearTimeout(timer); resolve(v); },
        reject:  e => { clearTimeout(timer); this.pending.delete(id); reject(e); },
      });
    });
  }
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function headersToObj(headers) {
  const obj = {};
  for (const [k, v] of headers) obj[k] = v;
  return obj;
}

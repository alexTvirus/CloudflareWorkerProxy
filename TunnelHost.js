/**
 * TunnelHost — Cloudflare Durable Object
 *
 * Mỗi instance đại diện cho một hostname duy nhất.
 * Instance tự quản lý toàn bộ vòng đời của tunnel:
 *   - Giữ WebSocket kết nối từ local tunnel client  (this.tunnelWs)
 *   - Nhận HTTP / WebSocket requests từ browser
 *   - Relay chúng qua tunnel tới local service
 *
 * Không cần biến global nào — state nằm hoàn toàn trong instance này.
 *
 * Protocol tunnel (JSON over WebSocket, thay thế socket.io events):
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  Server → Client                                                    │
 * │  { type: 'request',            requestId, data: {method,hdrs,path}}│
 * │  { type: 'request-pipe',       requestId, data: <base64> }         │
 * │  { type: 'request-pipe-end',   requestId }                         │
 * │  { type: 'request-pipe-error', requestId, data: <msg> }            │
 * │                                                                     │
 * │  Client → Server                                                    │
 * │  { type: 'response',           requestId, data: {status,hdrs,...} }│
 * │  { type: 'response-pipe',      requestId, data: <base64> }  ← cả  │
 * │  { type: 'response-pipe-end',  requestId, data?: <base64> }   hai  │
 * │  { type: 'response-pipe-error',requestId, data: <msg> }      chiều │
 * │  { type: 'request-error',      requestId, data: <msg> }      WS    │
 * │  { type: 'ping' }  ↔  { type: 'pong' }                            │
 * └─────────────────────────────────────────────────────────────────────┘
 */

// ─────────────────────────────────────────────────────────────────────────────
// EventEmitter thuần JS
// ─────────────────────────────────────────────────────────────────────────────
class EventEmitter {
  constructor() {
    this._events = Object.create(null);
  }

  on(event, listener) {
    (this._events[event] ??= []).push(listener);
    return this;
  }

  off(event, listener) {
    if (!this._events[event]) return this;
    this._events[event] = this._events[event].filter(
      (l) => l !== listener && l._orig !== listener,
    );
    return this;
  }

  once(event, listener) {
    const wrapper = (...args) => {
      this.off(event, wrapper);
      listener(...args);
    };
    wrapper._orig = listener;
    return this.on(event, wrapper);
  }

  emit(event, ...args) {
    if (!this._events[event]) return false;
    // Sao chép trước khi iterate — listener có thể gọi off() trong khi chạy
    [...this._events[event]].forEach((l) => l(...args));
    return true;
  }

  removeAllListeners(event) {
    if (event) delete this._events[event];
    else this._events = Object.create(null);
    return this;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// JWT — HS256 dùng Web Crypto API (thay jsonwebtoken)
// ─────────────────────────────────────────────────────────────────────────────
function b64urlEncode(str) {
  return btoa(str).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

function b64urlDecode(str) {
  str = str.replace(/-/g, '+').replace(/_/g, '/');
  while (str.length % 4) str += '=';
  return atob(str);
}

async function importHmacKey(secret, usage) {
  return crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    [usage],
  );
}



// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/** Uint8Array → base64 string */
const encodeB64 = (bytes) => btoa(String.fromCharCode(...bytes));

/** base64 string → Uint8Array */
const decodeB64 = (str) => Uint8Array.from(atob(str), (c) => c.charCodeAt(0));

/** Gửi JSON message tới WebSocket */
const wsSend = (ws, msg) => ws.send(JSON.stringify(msg));

/** Xây dựng headers với X-Forwarded-* từ request gốc */
function buildForwardedHeaders(request) {
  const headers = {};
  for (const [k, v] of request.headers) headers[k] = v;

  const url   = new URL(request.url);
  const proto = url.protocol.replace(':', '') || 'http';
  const host  = headers['host'] || url.hostname;
  const port  = url.port || (proto === 'https' ? '443' : '80');
  const ip    = headers['cf-connecting-ip'] || headers['x-real-ip'] || '0.0.0.0';

  for (const [key, val] of [['for', ip], ['port', port], ['proto', proto]]) {
    const prev = headers[`x-forwarded-${key}`];
    headers[`x-forwarded-${key}`] = prev ? `${prev},${val}` : String(val);
  }
  headers['x-forwarded-host'] = headers['x-forwarded-host'] || host;
  return headers;
}

// ─────────────────────────────────────────────────────────────────────────────
// TunnelHost Durable Object
// ─────────────────────────────────────────────────────────────────────────────
export class TunnelHost {
  /**
   * @param {DurableObjectState} state
   * @param {{ SECRET_KEY: string, VERIFY_TOKEN: string,
   *           JWT_GENERATOR_USERNAME: string, JWT_GENERATOR_PASSWORD: string }} env
   */
  constructor(state, env) {
    this.state   = state;
    this.env     = env;

    /** WebSocket của local tunnel client. null = chưa có tunnel nào kết nối. */
    this.tunnelWs = null;

    /**
     * EventEmitter dùng để route messages nhận từ tunnelWs
     * tới đúng handler đang chờ (handleHttpProxy / handleWsProxy).
     * Thay thế hoàn toàn socket.io event system.
     */
    this.emitter = new EventEmitter();
  }

  // Đọc config từ env binding (CF Dashboard) hoặc fallback hardcode
  get SECRET_KEY()             { return this.env.SECRET_KEY             ?? 'abc'; }
  get VERIFY_TOKEN()           { return this.env.VERIFY_TOKEN           ?? 'abc'; }
  get JWT_GENERATOR_USERNAME() { return this.env.JWT_GENERATOR_USERNAME ?? 'abc'; }
  get JWT_GENERATOR_PASSWORD() { return this.env.JWT_GENERATOR_PASSWORD ?? 'abc'; }

  // ── Router ────────────────────────────────────────────────────────────────
  async fetch(request) {
    const url     = new URL(request.url);
    const upgrade = (request.headers.get('upgrade') || '').toLowerCase();

    // ① Kết nối tunnel từ local client
    if (url.pathname.startsWith('/$web_tunnel')) {
      if (upgrade !== 'websocket') {
        return new Response('Expected WebSocket upgrade', {
          status: 426,
          headers: { Upgrade: 'websocket' },
        });
      }
      return this._handleTunnelConnect(request);
    }

    // ② Endpoint lấy JWT token (cho local client)
    if (url.pathname === '/tunnel_jwt_generator') {
      return this._handleJwtGenerator(url);
    }

    // ③ Proxy WebSocket từ browser qua tunnel
    if (upgrade === 'websocket') {
      return this._handleWsProxy(request);
    }

    // ④ Proxy HTTP request qua tunnel
    return this._handleHttpProxy(request);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // ① Tunnel connect: local client kết nối WebSocket vào server
  //
  //   local-client ──WS── DO.tunnelWs ──emitter events──► pending handlers
  // ─────────────────────────────────────────────────────────────────────────
  async _handleTunnelConnect(request) {
    const url = new URL(request.url);

    // ── Tạo WebSocket pair ────────────────────────────────────────────────
    // client  → trả về cho local tunnel client (họ giữ kết nối về local service)
    // server  → DO giữ lại để relay messages
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();

    this.tunnelWs = server;
    console.log('[TunnelHost] tunnel client connected');

    // Nhận JSON messages từ tunnel client → phát ra emitter events
    server.addEventListener('message', ({ data }) => {
      try {
        const raw = typeof data === 'string' ? data : new TextDecoder().decode(data);
        const msg = JSON.parse(raw);

        if (msg.type === 'ping') {
          server.send(JSON.stringify({ type: 'pong' }));
          return;
        }

        // Phát event với signature: (requestId, payload)
        // Tất cả handlers đang chờ sẽ lọc theo requestId của mình
        this.emitter.emit(msg.type, msg.requestId, msg.data);
      } catch (e) {
        console.error('[TunnelHost] message parse error:', e.message);
      }
    });

    // Cleanup khi tunnel đóng kết nối
    const onClose = () => {
      console.log('[TunnelHost] tunnel client disconnected');
      this.tunnelWs = null;
      // Thông báo tất cả handlers đang pending để trả về 502
      this.emitter.emit('disconnect');
      this.emitter.removeAllListeners();
    };
    server.addEventListener('close', onClose);
    server.addEventListener('error', onClose);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // ② JWT generator
  // ─────────────────────────────────────────────────────────────────────────
  async _handleJwtGenerator(url) {
    return new Response("r", { status: 200 });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // ③ HTTP proxy
  //
  //   browser ──HTTP──► DO ──JSON/WS──► tunnelWs ──► local-client ──► local service
  //   browser ◄──────── DO ◄──────────────────────────────────────────────────────
  // ─────────────────────────────────────────────────────────────────────────
  async _handleHttpProxy(request) {
    if (!this.tunnelWs) {
      return new Response('No tunnel connected for this host', { status: 404 });
    }

    const ws        = this.tunnelWs;
    const emitter   = this.emitter;
    const requestId = crypto.randomUUID();
    const url       = new URL(request.url);

    // 1. Gửi request metadata
    wsSend(ws, {
      type:      'request',
      requestId,
      data: {
        method:  request.method,
        headers: buildForwardedHeaders(request),
        path:    url.pathname + url.search,
      },
    });

    // 2. Stream request body tới tunnel client
    //    Chạy song song với việc lắng nghe response (fire-and-forget)
    (async () => {
      if (!request.body) {
        wsSend(ws, { type: 'request-pipe-end', requestId });
        return;
      }
      try {
        const reader = request.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          wsSend(ws, { type: 'request-pipe', requestId, data: encodeB64(value) });
        }
        wsSend(ws, { type: 'request-pipe-end', requestId });
      } catch (e) {
        wsSend(ws, { type: 'request-pipe-error', requestId, data: e.message });
      }
    })();

    // 3. Chờ response headers + stream response body về browser
    //    Dùng TransformStream làm cầu nối: emitter events → ReadableStream
    return new Promise((resolve) => {
      const { readable, writable } = new TransformStream();
      const writer   = writable.getWriter();
      let   resolved = false;

      // Gỡ tất cả listeners khi request kết thúc (thành công hay lỗi)
      const cleanup = () => {
        emitter.off('response',            onResponse);
        emitter.off('response-pipe',       onPipe);
        emitter.off('response-pipe-end',   onPipeEnd);
        emitter.off('response-pipe-error', onPipeError);
        emitter.off('request-error',       onReqError);
        emitter.off('disconnect',          onDisconnect);
      };

      // Nhận response headers từ tunnel client → resolve Promise với Response
      const onResponse = (id, data) => {
        if (id !== requestId) return;
        emitter.off('response',      onResponse);
        emitter.off('request-error', onReqError);
        resolved = true;
        resolve(
          new Response(readable, {
            status:     data.statusCode,
            statusText: data.statusMessage || '',
            headers:    data.headers       || {},
          }),
        );
      };

      // Nhận chunk body → đẩy vào TransformStream writer
      const onPipe = (id, data) => {
        if (id !== requestId || !data) return;
        writer.write(decodeB64(data)).catch(() => {});
      };

      // Body kết thúc → đóng writer (browser nhận được response hoàn chỉnh)
      const onPipeEnd = (id, data) => {
        if (id !== requestId) return;
        if (data) writer.write(decodeB64(data)).catch(() => {});
        writer.close().catch(() => {});
        cleanup();
      };

      // Lỗi stream body từ phía local service
      const onPipeError = (id, error) => {
        if (id !== requestId) return;
        writer.abort(new Error(error || 'Stream error')).catch(() => {});
        cleanup();
      };

      // Local service không phản hồi được (connection refused, timeout, ...)
      const onReqError = (id, error) => {
        if (id !== requestId) return;
        cleanup();
        if (!resolved) {
          resolved = true;
          resolve(new Response(`Bad Gateway: ${error || 'upstream error'}`, { status: 502 }));
        } else {
          writer.abort(new Error(error || 'Request error')).catch(() => {});
        }
      };

      // Tunnel WebSocket bị đóng đột ngột trong khi đang xử lý
      const onDisconnect = () => {
        cleanup();
        if (!resolved) {
          resolved = true;
          resolve(new Response('Tunnel disconnected', { status: 502 }));
        } else {
          writer.abort(new Error('Tunnel disconnected')).catch(() => {});
        }
      };

      emitter.on('response',            onResponse);
      emitter.on('response-pipe',       onPipe);
      emitter.on('response-pipe-end',   onPipeEnd);
      emitter.on('response-pipe-error', onPipeError);
      emitter.on('request-error',       onReqError);
      emitter.on('disconnect',          onDisconnect);
    });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // ④ WebSocket proxy (bidirectional relay)
  //
  //   browser ──WS──► serverWs ══JSON/tunnel══► tunnelWs ──► local service
  //   browser ◄──WS── serverWs ◄═JSON/tunnel══ tunnelWs ◄── local service
  //
  //   Dùng lại event "response-pipe" cho cả hai chiều (giống pipe bidirectional
  //   trong bản gốc: tunnelResponse.pipe(socket).pipe(tunnelResponse))
  // ─────────────────────────────────────────────────────────────────────────
  async _handleWsProxy(request) {
    if (!this.tunnelWs) {
      return new Response('No tunnel connected for this host', { status: 404 });
    }

    const tunnelWs  = this.tunnelWs;
    const emitter   = this.emitter;
    const requestId = crypto.randomUUID();
    const url       = new URL(request.url);

    // Gửi yêu cầu WS upgrade tới tunnel client (local side)
    wsSend(tunnelWs, {
      type:      'request',
      requestId,
      data: {
        method:  request.method,
        headers: buildForwardedHeaders(request),
        path:    url.pathname + url.search,
      },
    });
    // Không có body với WS upgrade
    wsSend(tunnelWs, { type: 'request-pipe-end', requestId });

    // Tạo WebSocket pair để trả về cho browser
    const webSocketPair = new WebSocketPair();
    const [client, serverWs] = Object.values(webSocketPair);
    serverWs.accept();

    // Thiết lập relay bất đồng bộ
    this._setupWsRelay(requestId, serverWs, tunnelWs, emitter);

    // Trả 101 ngay — relay diễn ra bất đồng bộ sau đó
    return new Response(null, { status: 101, webSocket: client });
  }

  /**
   * Thiết lập bidirectional relay giữa browser WebSocket và tunnel WebSocket.
   *
   * Giai đoạn 1 — Chờ tunnel client xác nhận upgrade (event 'response'):
   *   Nếu local service trả về HTTP error → đóng serverWs
   *   Nếu upgrade thành công (statusCode null hoặc 101) → sang giai đoạn 2
   *
   * Giai đoạn 2 — Relay messages hai chiều:
   *   browser → serverWs.message → tunnelSend 'response-pipe' → local service
   *   local service → tunnel 'response-pipe' event → serverWs.send → browser
   */
  _setupWsRelay(requestId, serverWs, tunnelWs, emitter) {
    let relayActive = false;

    // Relay listeners — chỉ được gán sau khi upgrade được xác nhận
    let onTunnelMsg = null;
    let onTunnelEnd = null;
    let onTunnelErr = null;

    const cleanup = () => {
      emitter.off('response',            onResponse);
      emitter.off('request-error',       onReqError);
      emitter.off('disconnect',          onDisconnect);
      if (onTunnelMsg) emitter.off('response-pipe',       onTunnelMsg);
      if (onTunnelEnd) emitter.off('response-pipe-end',   onTunnelEnd);
      if (onTunnelErr) emitter.off('response-pipe-error', onTunnelErr);
    };

    // Local service báo lỗi request
    const onReqError = (id) => {
      if (id !== requestId) return;
      cleanup();
      serverWs.close(1011, 'Upstream request error');
    };

    // Tunnel WebSocket bị đóng giữa chừng
    const onDisconnect = () => {
      cleanup();
      serverWs.close(1011, 'Tunnel disconnected');
    };

    // ── Giai đoạn 1: nhận response/upgrade confirmation từ tunnel ─────────
    const onResponse = (id, data) => {
      if (id !== requestId) return;
      emitter.off('response',      onResponse);
      emitter.off('request-error', onReqError);

      // Local service từ chối upgrade (trả HTTP error)
      if (data?.statusCode && data.statusCode !== 101) {
        serverWs.close(1011, `Upstream HTTP ${data.statusCode}`);
        cleanup();
        return;
      }

      // ── Giai đoạn 2: relay bật hai chiều ─────────────────────────────
      relayActive = true;

      // Tunnel → Browser: local service gửi WS message → forward tới browser
      onTunnelMsg = (id, msgData) => {
        if (id !== requestId || msgData == null) return;
        try {
          serverWs.send(decodeB64(msgData));
        } catch (_) {}
      };

      // Local service đóng WS kết nối
      onTunnelEnd = (id) => {
        if (id !== requestId) return;
        cleanup();
        serverWs.close(1000, 'Upstream closed');
      };

      // Local service báo lỗi stream
      onTunnelErr = (id, err) => {
        if (id !== requestId) return;
        cleanup();
        serverWs.close(1011, err || 'Upstream stream error');
      };

      emitter.on('response-pipe',       onTunnelMsg);
      emitter.on('response-pipe-end',   onTunnelEnd);
      emitter.on('response-pipe-error', onTunnelErr);

      // Browser → Tunnel: browser gửi WS message → forward tới local service
      serverWs.addEventListener('message', ({ data: msgData }) => {
        if (!relayActive) return;
        try {
          const encoded =
            typeof msgData === 'string'
              ? encodeB64(new TextEncoder().encode(msgData))
              : encodeB64(new Uint8Array(msgData));
          // Dùng 'response-pipe' — đối xứng với chiều ngược lại
          wsSend(tunnelWs, { type: 'response-pipe', requestId, data: encoded });
        } catch (_) {}
      });

      // Browser đóng kết nối → thông báo tới local service qua tunnel
      serverWs.addEventListener('close', () => {
        if (!relayActive) return;
        relayActive = false;
        cleanup();
        wsSend(tunnelWs, { type: 'response-pipe-end', requestId });
      });
    };

    emitter.on('response',      onResponse);
    emitter.on('request-error', onReqError);
    emitter.on('disconnect',    onDisconnect);
  }
}

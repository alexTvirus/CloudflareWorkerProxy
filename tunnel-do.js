// src/tunnel-do.js — Durable Object giữ kết nối tunnel
// Deploy lên Cloudflare Workers
// Sử dụng Cloudflare WebSocket Hibernation API để giữ kết nối ổn định,
// không bị Cloudflare tắt giữa chừng khi DO idle.

// ─── Binary Frame Protocol ────────────────────────────────────────────────────
//
//  Layout (tất cả offset tính từ đầu frame):
//  [0]       uint8   – type
//  [1..36]   ascii   – id (UUID 36 chars, hoặc ZERO_ID cho PING/PONG)
//  [37..40]  uint32BE – metaLen (số byte của phần JSON metadata)
//  [41..]    bytes   – meta JSON  (metaLen byte)
//  [41+metaLen..] bytes – payload nhị phân (phần còn lại của frame)
//
const HEADER    = 41;          // bytes cố định ở đầu mỗi frame
const CHUNK_SZ  = 256 * 1024; // 256 KB – kích thước mỗi chunk request body
const PING_MS   = 20_000;     // gửi PING mỗi 20 giây

const ZERO_ID = '00000000-0000-0000-0000-000000000000';

// Tag định danh loại WebSocket – dùng bởi Hibernation API
const TAG_TUNNEL = 'tunnel';
const tagClient  = id => `client:${id}`;

/** Loại message (giống nhau ở cả DO và client) */
const M = Object.freeze({
  HTTP_REQ_HEAD:  0x01, // DO→A  meta={method,url,headers}
  HTTP_REQ_CHUNK: 0x02, // DO→A  payload=body chunk
  HTTP_REQ_END:   0x03, // DO→A  meta={cancel?}
  HTTP_RES_HEAD:  0x04, // A→DO  meta={status,headers}
  HTTP_RES_CHUNK: 0x05, // A→DO  payload=body chunk
  HTTP_RES_END:   0x06, // A→DO  meta={ok,error?}
  WS_CONNECT:     0x07, // DO→A  meta={url,headers}
  WS_ACCEPT:      0x08, // A→DO  meta={}
  WS_REJECT:      0x09, // A→DO  meta={}
  WS_DATA:        0x0A, // bidirectional  meta={binary}  payload=data
  WS_CLOSE:       0x0B, // bidirectional
  PING:           0x0C,
  PONG:           0x0D,
});

// ─── Encode / Decode (Web API – hoạt động trên CF Workers) ───────────────────

const _te = new TextEncoder();
const _td = new TextDecoder();

/**
 * Tạo một binary frame hoàn chỉnh (trả về ArrayBuffer).
 */
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
  dv.setUint32(37, mBuf.length, false /*big-endian*/);
  out.set(mBuf, HEADER);
  out.set(pBuf, HEADER + mBuf.length);
  return out.buffer;
}

/**
 * Phân tích một binary frame.
 * Luôn tạo bản sao của payload để tránh giữ ref vào buffer gốc (GC-safe).
 */
function decode(raw) {
  // Chuẩn hoá về Uint8Array offset-0 để tránh bug với DataView byteOffset
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
  // .slice() tạo bản sao – an toàn khi buffer gốc được tái sử dụng
  const payload = u8.slice(HEADER + metaLen);

  return { type, id, meta, payload };
}

// ─── Durable Object ───────────────────────────────────────────────────────────

export class TunnelDO {
  constructor(state, env) {
    this.state  = state;
    this.env    = env;

    // ── LƯU Ý VỀ HIBERNATION ──────────────────────────────────────────────────
    // Khi DO hibernate, toàn bộ in-memory state (pending, clients) bị mất.
    // Tuy nhiên với tunnel này:
    //   • this.pending  – chỉ tồn tại trong scope của một request đang xử lý;
    //     request đó giữ DO awake cho đến khi hoàn thành → không bị mất.
    //   • this.clients  – WebSocket của client B cũng được đăng ký qua
    //     ctx.acceptWebSocket nên được khôi phục sau hibernate qua getWebSockets().
    //     Map này được re-populate lazily trong _getClientWs().
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Map id → { resolve, reject, writer? }
     *
     * - HTTP request: writer được gán khi nhận HTTP_RES_HEAD
     * - WS: resolve/reject dùng một lần rồi xoá
     */
    this.pending = new Map();

    /**
     * Cache id → WebSocket (client B).
     * Được populate từ ctx.getWebSockets(tagClient(id)) khi cần.
     */
    this.clients = new Map();
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

  /**
   * Trả về WebSocket tunnel đang hoạt động (nếu có).
   * Dùng ctx.getWebSockets(tag) thay vì this.tunnel để an toàn sau hibernate.
   */
  _getTunnelWs() {
    const sockets = this.state.getWebSockets(TAG_TUNNEL);
    return sockets.length > 0 ? sockets[0] : null;
  }

  /**
   * Trả về WebSocket của client B theo id.
   * Tìm trong cache trước, nếu không có thì hỏi runtime.
   */
  _getClientWs(id) {
    if (this.clients.has(id)) return this.clients.get(id);
    const sockets = this.state.getWebSockets(tagClient(id));
    if (sockets.length > 0) {
      this.clients.set(id, sockets[0]);
      return sockets[0];
    }
    return null;
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

    // ── HIBERNATION API ──────────────────────────────────────────────────────
    // Thay server.accept() bằng ctx.acceptWebSocket(server, tags).
    // Runtime CF quản lý vòng đời; DO có thể hibernate khi không có I/O
    // và được đánh thức tự động khi có message mới đến.
    // Tag 'tunnel' dùng để lấy lại socket sau khi wake-up.
    this.state.acceptWebSocket(server, [TAG_TUNNEL]);
    // ────────────────────────────────────────────────────────────────────────

    // Keepalive qua Alarm thay vì setInterval (setInterval không tồn tại sau hibernate)
    this._scheduleNextPing();

    console.log('[DO] Tunnel client A kết nối');
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Đóng tunnel chủ động (reset hoặc bị thay thế). */
  _closeTunnel(reason) {
    // Huỷ alarm ping nếu đang chờ
    this.state.storage.deleteAlarm().catch(() => {});

    const ws = this._getTunnelWs();
    if (ws) {
      try { ws.close(1000, reason); } catch {}
    }
  }

  /** Xử lý khi tunnel bị đóng (lỗi hoặc client A disconnect). */
  _onTunnelClose() {
    this.state.storage.deleteAlarm().catch(() => {});
    console.log('[DO] Tunnel client A ngắt kết nối');

    // Huỷ tất cả streaming response đang mở
    for (const [, p] of this.pending) {
      if (p.writer) {
        p.writer.abort(new Error('Tunnel disconnected')).catch(() => {});
      } else {
        p.reject(new Error('Tunnel disconnected'));
      }
    }
    this.pending.clear();

    // Đóng tất cả WebSocket client B còn đang sống
    for (const ws of this.state.getWebSockets()) {
      // Bỏ qua socket có tag 'tunnel' (đã đóng rồi)
      const tags = ws.tags ?? [];
      if (tags.includes(TAG_TUNNEL)) continue;
      try { ws.close(1001, 'Tunnel disconnected'); } catch {}
    }
    this.clients.clear();
  }

  // ─── Hibernation API – message handler (thay addEventListener) ─────────────

  /**
   * Được CF runtime gọi khi BẤT KỲ WebSocket nào được acceptWebSocket nhận message.
   * Phân biệt tunnel vs client B qua tag.
   */
  webSocketMessage(ws, message) {
    const tags = ws.tags ?? [];

    if (tags.includes(TAG_TUNNEL)) {
      // Message từ client A (tunnel)
      this._onTunnelMsg(message);
    } else {
      // Message từ client B (WebSocket proxy)
      // Tìm id từ tag 'client:<id>'
      const clientTag = tags.find(t => t.startsWith('client:'));
      if (!clientTag) return;
      const id = clientTag.slice(7); // bỏ 'client:'

      const tunnel = this._getTunnelWs();
      if (!tunnel) return;

      const binary = message instanceof ArrayBuffer;
      this._tunnelSend(encode(M.WS_DATA, id, { binary },
        binary ? new Uint8Array(message) : _te.encode(message)));
    }
  }

  /**
   * Được CF runtime gọi khi WebSocket đóng (thay addEventListener 'close').
   */
  webSocketClose(ws, code, reason, wasClean) {
    const tags = ws.tags ?? [];

    if (tags.includes(TAG_TUNNEL)) {
      this._onTunnelClose();
    } else {
      const clientTag = tags.find(t => t.startsWith('client:'));
      if (!clientTag) return;
      const id = clientTag.slice(7);

      this.clients.delete(id);

      const tunnel = this._getTunnelWs();
      if (tunnel) {
        try { this._tunnelSend(encode(M.WS_CLOSE, id, null, null)); } catch {}
      }
    }
  }

  /**
   * Được CF runtime gọi khi WebSocket có lỗi (thay addEventListener 'error').
   */
  webSocketError(ws, error) {
    const tags = ws.tags ?? [];

    if (tags.includes(TAG_TUNNEL)) {
      console.error('[DO] Tunnel WebSocket lỗi:', error);
      this._onTunnelClose();
    } else {
      const clientTag = tags.find(t => t.startsWith('client:'));
      if (clientTag) {
        const id = clientTag.slice(7);
        this.clients.delete(id);
        const tunnel = this._getTunnelWs();
        if (tunnel) {
          try { this._tunnelSend(encode(M.WS_CLOSE, id, null, null)); } catch {}
        }
      }
    }
  }

  // ─── Alarm – thay thế setInterval cho PING (Hibernation-safe) ─────────────

  /**
   * Lên lịch alarm tiếp theo để gửi PING.
   * Alarm tồn tại qua hibernate, không như setInterval.
   */
  _scheduleNextPing() {
    this.state.storage.setAlarm(Date.now() + PING_MS).catch(() => {});
  }

  /**
   * CF runtime gọi alarm() khi đến giờ.
   * Gửi PING nếu tunnel còn sống, sau đó lên lịch alarm tiếp theo.
   */
  async alarm() {
    const tunnel = this._getTunnelWs();
    if (tunnel) {
		try {  tunnel.ping();} catch {}
      try { tunnel.send(encode(M.PING, ZERO_ID, null, null)); } catch {}
      // Tiếp tục lên lịch alarm kế
      this._scheduleNextPing();
    }
    // Nếu không còn tunnel thì không lên lịch nữa → alarm tự dừng
  }

  // ─── Dispatch message từ client A ─────────────────────────────────────────

  _onTunnelMsg(raw) {
    let f;
    try { f = decode(raw); } catch (e) {
      console.error('[DO] decode lỗi:', e.message);
      return;
    }

    switch (f.type) {

      // ── Keepalive ──────────────────────────────────────────────────────────
      case M.PING: {
        const tunnel = this._getTunnelWs();
        try { tunnel?.send(encode(M.PONG, ZERO_ID, null, null)); } catch {}
        break;
      }
      case M.PONG:
        break;

      // ── HTTP streaming response (A → DO → client B) ───────────────────────

      /**
       * Nhận response headers từ client A.
       * Tạo TransformStream: writable end nhận chunks từ tunnel,
       * readable end được đưa thẳng vào Response body → CF tự stream đến client B.
       * Không buffer toàn bộ body vào memory.
       */
      case M.HTTP_RES_HEAD: {
        const p = this.pending.get(f.id);
        if (!p || p.writer) break;

        const { readable, writable } = new TransformStream();
        p.writer = writable.getWriter();

        // resolve ngay với Response streaming – không cần chờ body xong
        p.resolve(new Response(readable, {
          status:  f.meta.status  || 200,
          headers: f.meta.headers || {},
        }));
        // GIỮ pending entry để nhận tiếp chunks và END
        break;
      }

      /**
       * Nhận một chunk của response body.
       * Ghi vào WritableStream writer (fire-and-forget).
       */
      case M.HTTP_RES_CHUNK: {
        const p = this.pending.get(f.id);
        if (p?.writer && f.payload.length > 0) {
          p.writer.write(f.payload).catch(() => {
            this.pending.delete(f.id);
          });
        }
        break;
      }

      /**
       * Kết thúc response body.
       */
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

      // ── WebSocket accept/reject ────────────────────────────────────────────
      case M.WS_ACCEPT:
      case M.WS_REJECT: {
        const p = this.pending.get(f.id);
        if (!p) break;
        this.pending.delete(f.id);
        p.resolve({ accepted: f.type === M.WS_ACCEPT });
        break;
      }

      // ── WebSocket data / close ─────────────────────────────────────────────
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

  // ─── HTTP proxy: client B → tunnel → client A ─────────────────────────────

  async _proxyHttp(request) {
    const id = crypto.randomUUID();

    // 1. Gửi request headers
    this._tunnelSend(encode(M.HTTP_REQ_HEAD, id, {
      method:  request.method,
      url:     request.url,
      headers: headersToObj(request.headers),
    }, null));

    // 2. Stream request body theo từng chunk
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

    // 3. Kết thúc request body
    this._tunnelSend(encode(M.HTTP_REQ_END, id, { cancel: false }, null));

    // 4. Chờ response headers
    let response;
    try {
      response = await this._waitFor(id, 60_000);
    } catch (e) {
      return new Response(e.message, { status: 502 });
    }

    return response;
  }

  // ─── WebSocket proxy: client B ↔ tunnel ↔ client A ────────────────────────

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

    // ── HIBERNATION API ──────────────────────────────────────────────────────
    // Đăng ký WebSocket client B với tag 'client:<id>' để:
    //   1. Không bị tắt bởi CF khi DO hibernate
    //   2. Có thể lấy lại socket bằng getWebSockets(tagClient(id))
    this.state.acceptWebSocket(server, [tagClient(id)]);
    // ────────────────────────────────────────────────────────────────────────

    this.clients.set(id, server);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /** Gửi frame qua tunnel, bỏ qua nếu tunnel đã đóng. */
  _tunnelSend(frame) {
    const tunnel = this._getTunnelWs();
    if (tunnel) {
      try { tunnel.send(frame); } catch {}
    }
  }

  /**
   * Chờ response/accept cho một request id cụ thể.
   */
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

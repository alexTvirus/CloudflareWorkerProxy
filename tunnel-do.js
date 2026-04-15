// tunnel-do-simple.js — Durable Object relay đơn giản
// 1 client A (/$web_tunnel) + 1 client B (/$web_client)
// Không có binary protocol header tự chế:
//   - text frame  → PING / PONG (control, xử lý nội bộ)
//   - binary frame → raw payload, forward thẳng, zero-copy
//
// Thứ tự và toàn vẹn byte:
//   WebSocket đảm bảo ordered delivery (TCP).
//   DO chỉ .send() lại nguyên ArrayBuffer — không cắt, không ghép.
//
// Deploy lên Cloudflare Workers với Hibernation API.

// ─── Hằng số ─────────────────────────────────────────────────────────────────

const TUNNEL_TOKEN = 'abc';          // token xác thực client A
const PING_MS      = 10_000;        // alarm interval (ms)

// Tag dùng với Hibernation API — getWebSockets(tag)
const TAG_A = 'side-a';             // client A (tunnel agent)
const TAG_B = 'side-b';             // client B (browser/app)

// Text frame payload cho PING / PONG
const MSG_PING = 'PING';
const MSG_PONG = 'PONG';

// ─── Durable Object ───────────────────────────────────────────────────────────

export class TunnelDO {
  constructor(state, env) {
    this.state = state;
    this.env   = env;
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  _getA() {
    const sockets = this.state.getWebSockets(TAG_A);
    return sockets.length > 0 ? sockets[0] : null;
  }

  _getB() {
    const sockets = this.state.getWebSockets(TAG_B);
    return sockets.length > 0 ? sockets[0] : null;
  }

  // Lấy tags của ws (Hibernation API — ws.tags không tồn tại sau hibernate)
  _tags(ws) {
    try { return this.state.getTags(ws) ?? []; } catch { return []; }
  }

  // ─── Router ────────────────────────────────────────────────────────────────

  async fetch(request) {
    const url     = new URL(request.url);
    const upgrade = request.headers.get('Upgrade');

    // Reset endpoint (tiện debug)
    if (url.pathname.includes('/reset')) {
      this._closeAll('Reset requested');
      return new Response('ok');
    }

    // Client A kết nối tunnel
    if (url.pathname === '/$web_tunnel') {
      return this._acceptA(request);
    }

    // Client B kết nối
    if (url.pathname === '/$web_client') {
      if (upgrade !== 'websocket') {
        return new Response('WebSocket required', { status: 426 });
      }
      return this._acceptB(request);
    }

    return new Response('Not found', { status: 404 });
  }

  // ─── Chấp nhận client A ───────────────────────────────────────────────────

  _acceptA(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('WebSocket required', { status: 426 });
    }

    // Xác thực token
    const token = request.headers.get('x-tunnel-token')
               || new URL(request.url).searchParams.get('token');
    if (TUNNEL_TOKEN && token !== TUNNEL_TOKEN) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Đóng kết nối A cũ nếu có (reconnect)
    const oldA = this._getA();
    if (oldA) {
      try { oldA.close(1000, 'Replaced by new connection'); } catch {}
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server, [TAG_A]);

    // Bắt đầu chu kỳ PING keepalive qua Alarm API
    this.state.storage.setAlarm(Date.now() + PING_MS).catch(() => {});

    console.log('[DO] Client A kết nối');
    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Chấp nhận client B ───────────────────────────────────────────────────

  _acceptB(request) {
    // Đóng B cũ nếu có
    const oldB = this._getB();
    if (oldB) {
      try { oldB.close(1000, 'Replaced by new connection'); } catch {}
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server, [TAG_B]);

    console.log('[DO] Client B kết nối');
    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Hibernation API handlers ──────────────────────────────────────────────

  webSocketMessage(ws, message) {
    const tags = this._tags(ws);

    // ── Text frame: chỉ xử lý PING/PONG, không forward ──────────────────────
    if (typeof message === 'string') {
      if (message === MSG_PING) {
        try { ws.send(MSG_PONG); } catch {}
      }
      // PONG → bỏ qua (latency tracking nếu cần có thể thêm ở đây)
      return;
    }

    // ── Binary frame: forward thẳng, zero-copy ───────────────────────────────
    // ArrayBuffer từ Hibernation API có thể là shared view → slice để an toàn
    const frame = null;
	  
	  if(message instanceof ArrayBuffer){
		  frame = message
	  }else{
		 let rawSlice = message.buffer.slice(message.byteOffset, message.byteOffset + message.byteLength);
		  frame = rawSlice.length > 0
			? Buffer.from(rawSlice) // ← bản sao, không phải view
			: Buffer.alloc(0);
	  }

    if (tags.includes(TAG_A)) {
      // A → B
      const b = this._getB();
      if (b) {
        try { b.send(frame); } catch {}
      }
      return;
    }

    if (tags.includes(TAG_B)) {
      // B → A
      const a = this._getA();
      if (a) {
        try { a.send(frame); } catch {}
      }
      return;
    }
  }

  webSocketClose(ws, code, reason) {
    const tags = this._tags(ws);
    const side = tags.includes(TAG_A) ? 'A' : tags.includes(TAG_B) ? 'B' : '?';
    console.log(`[DO] Client ${side} ngắt kết nối (${code})`);

    if (tags.includes(TAG_A)) {
      // A đóng → dừng alarm, báo B
      this.state.storage.deleteAlarm().catch(() => {});
      const b = this._getB();
      if (b) try { b.close(1001, 'Tunnel disconnected'); } catch {}
      return;
    }

    if (tags.includes(TAG_B)) {
      // B đóng → báo A (text frame nhỏ thay vì binary custom protocol)
      const a = this._getA();
      if (a) try { a.send('CLIENT_DISCONNECTED'); } catch {}
      return;
    }
  }

  webSocketError(ws, error) {
    const tags = this._tags(ws);
    const side = tags.includes(TAG_A) ? 'A' : tags.includes(TAG_B) ? 'B' : '?';
    console.error(`[DO] Client ${side} lỗi:`, error?.message ?? error);
    // Không gọi ws.close() trên WS đã lỗi — chỉ dọn dẹp phía kia
    this.webSocketClose(ws, 1006, 'error');
  }

  // ─── Alarm — PING keepalive ────────────────────────────────────────────────
  // Dùng text frame thay vì binary có header → client A phân biệt bằng typeof

  async alarm() {
    const a = this._getA();
    if (!a) return; // Tunnel mất → dừng alarm

    try {
      a.send(MSG_PING); // text frame
    } catch {
      // send lỗi → tunnel đã chết, dọn dẹp
      this.state.storage.deleteAlarm().catch(() => {});
      const b = this._getB();
      if (b) try { b.close(1001, 'Tunnel disconnected'); } catch {}
      return;
    }

    this.state.storage.setAlarm(Date.now() + PING_MS).catch(() => {});
  }

  // ─── Đóng tất cả ─────────────────────────────────────────────────────────

  _closeAll(reason) {
    this.state.storage.deleteAlarm().catch(() => {});
    for (const ws of this.state.getWebSockets()) {
      try { ws.close(1000, reason); } catch {}
    }
  }
}

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
//
// ─── CÁC LỖI ĐÃ SỬA (so với bản gốc) ───────────────────────────────────────
// [FIX-1] webSocketMessage: text frame không khớp PING/LOCAL_DISCONNECTED sẽ
//         "rơi" xuống code xử lý binary phía dưới → ném exception vì string
//         không có .buffer. Đã thêm `return` đầy đủ cho mọi nhánh text.
// [FIX-2] _acceptA/_acceptB đóng socket cũ khi có reconnect (oldA/oldB.close()).
//         Việc đóng này lại trigger webSocketClose() như một disconnect THẬT,
//         khiến DO báo "CLIENT_DISCONNECTED" cho A (hoặc đóng B) ngay khi B
//         (hoặc A) chỉ đang... reconnect bình thường — tự phá vỡ khả năng
//         resume. Đã thêm kiểm tra "socket này còn là socket hiện hành của
//         tag đó không" trước khi coi đó là một disconnect thật.
// [FIX-3] Trích payload binary: nhánh else dùng `rawSlice.length` nhưng
//         rawSlice là ArrayBuffer (chỉ có byteLength) → luôn rơi vào
//         Buffer.alloc(0), mất dữ liệu nếu message không phải instanceof
//         ArrayBuffer. Đã sửa dùng byteLength và đảm bảo copy thật.
// [FIX-4] Thêm bounded buffer: nếu A/B tạm vắng mặt (đang reconnect) khi có
//         frame cần forward, không drop thẳng nữa mà giữ lại một lượng nhỏ
//         và flush ngay khi phía đó quay lại — giảm phụ thuộc vào cơ chế
//         retransmit ở lớp ReliableChannel phía trên.
//         Lưu ý: buffer này chỉ nằm trong memory của DO, sẽ mất nếu DO bị
//         hibernate/evict — đây là lớp "best-effort", không phải nguồn đảm
//         bảo tin cậy chính (nguồn đảm bảo chính vẫn là ReliableChannel
//         end-to-end ở client-local-cf.js / server-local-cf.js).

// ─── Hằng số ─────────────────────────────────────────────────────────────────

const TUNNEL_TOKEN = 'abc';          // token xác thực client A
const PING_MS      = 10_000;        // alarm interval (ms)

// Tag dùng với Hibernation API — getWebSockets(tag)
const TAG_A = 'side-a';             // client A (tunnel agent)
const TAG_B = 'side-b';             // client B (browser/app)

// Text frame payload cho PING / PONG
const MSG_PING = 'PING';
const MSG_PONG = 'PONG';
const LOCAL_DISCONNECTED = 'LOCAL_DISCONNECTED';

// Giới hạn buffer "chờ phía kia reconnect" — tránh phình memory vô hạn
const MAX_FAILOVER_BUFFER_BYTES = 4 * 1024 * 1024; // 2 MB mỗi hướng

// ─── Durable Object ───────────────────────────────────────────────────────────

export class TunnelDO {
  constructor(state, env) {
    this.state = state;
    this.env   = env;

    // [FIX-4] Hàng đợi nhỏ giữ frame khi A/B tạm vắng mặt giữa lúc reconnect.
    // Lưu ý: KHÔNG persist qua hibernate — chỉ là tối ưu best-effort.
    this._pendingToA = [];
    this._pendingToB = [];
    this._pendingToABytes = 0;
    this._pendingToBBytes = 0;
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

  // [FIX-4] Đẩy frame vào buffer chờ, cắt bỏ phần cũ nhất nếu vượt giới hạn
  _queuePending(queueName, frame) {
    const queue = this[queueName];
    const bytesKey = queueName + 'Bytes';
    queue.push(frame);
    this[bytesKey] += frame.byteLength ?? frame.length ?? 0;
    while (this[bytesKey] > MAX_FAILOVER_BUFFER_BYTES && queue.length > 0) {
      const dropped = queue.shift();
      this[bytesKey] -= dropped.byteLength ?? dropped.length ?? 0;
    }
  }

  _flushPending(queueName, socket) {
    const queue = this[queueName];
    const bytesKey = queueName + 'Bytes';
    while (queue.length > 0) {
      const frame = queue.shift();
      try { socket.send(frame); } catch { /* phía nhận lại vừa rớt, bỏ qua */ }
    }
    this[bytesKey] = 0;
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

    // Đóng kết nối A cũ nếu có (reconnect).
    // [FIX-2] Code đóng (4900) đánh dấu "bị thay thế" — webSocketClose() sẽ
    // tự nhận ra qua việc _getA() đã trả về socket MỚI, nên sẽ bỏ qua, nhưng
    // ta vẫn dùng code riêng để log rõ ràng hơn khi debug.
    const oldA = this._getA();
    if (oldA) {
      try { oldA.close(4900, 'Replaced by new connection'); } catch {}
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server, [TAG_A]);

    // Bắt đầu chu kỳ PING keepalive qua Alarm API
    this.state.storage.setAlarm(Date.now() + PING_MS).catch(() => {});

    // [FIX-4] A vừa quay lại → flush ngay các frame B→A đang chờ
    if (this._pendingToA.length > 0) {
      this._flushPending('_pendingToA', server);
    }

    console.log('[DO] Client A kết nối');
    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Chấp nhận client B ───────────────────────────────────────────────────

  _acceptB(request) {
    // Đóng B cũ nếu có — xem [FIX-2] ở _acceptA
    const oldB = this._getB();
    if (oldB) {
      try { oldB.close(4900, 'Replaced by new connection'); } catch {}
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server, [TAG_B]);

    // [FIX-4] B vừa quay lại → flush ngay các frame A→B đang chờ
    if (this._pendingToB.length > 0) {
      this._flushPending('_pendingToB', server);
    }

    console.log('[DO] Client B kết nối');
    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Hibernation API handlers ──────────────────────────────────────────────

  webSocketMessage(ws, message) {
    const tags = this._tags(ws);

    // ── Text frame: chỉ xử lý PING/PONG/LOCAL_DISCONNECTED, không forward ───
    // [FIX-1] Luôn return sau khi xử lý text — không để rơi xuống phần binary.
    if (typeof message === 'string') {
      if (message === MSG_PING) {
        try { ws.send(MSG_PONG); } catch {}
        return;
      }
      if (message === MSG_PONG) {
        // Latency tracking nếu cần có thể thêm ở đây
        return;
      }
      if (message === LOCAL_DISCONNECTED) {
        const b = this._getB();
        if (b) try { b.close(1001, 'LOCAL_DISCONNECTED'); } catch {}
        return;
      }
	  if (tags.includes(TAG_A)) {
			const b = this._getB();
			if (b) try { b.send(message); } catch {}
		} else if (tags.includes(TAG_B)) {
			const a = this._getA();
			if (a) try { a.send(message); } catch {}
		} else {
			console.warn('[DO] text frame từ side không xác định:', message);
		}
      console.warn('[DO] text frame không rõ:', message);
      return;
    }

    // ── Binary frame: forward thẳng, zero-copy ───────────────────────────────
    // [FIX-3] message luôn là ArrayBuffer theo Hibernation API; nhánh else chỉ
    // là phòng hộ — sửa dùng byteLength và copy thật (qua Uint8Array) để an
    // toàn nếu runtime đưa vào một TypedArray/Buffer thay vì ArrayBuffer.
    let frame;
    if (message instanceof ArrayBuffer) {
      frame = message;
    } else if (message && typeof message.byteLength === 'number') {
      frame = message.byteLength > 0
        ? Buffer.from(new Uint8Array(message.buffer, message.byteOffset, message.byteLength)) // ← bản sao thật
        : Buffer.alloc(0);
    } else {
      console.warn('[DO] binary frame không nhận diện được kiểu, bỏ qua');
      return;
    }

    if (tags.includes(TAG_A)) {
      // A → B
      const b = this._getB();
      if (b) {
        try { b.send(frame); } catch { this._queuePending('_pendingToB', frame); }
      } else {
        // [FIX-4] B đang vắng mặt (có thể đang reconnect) → giữ tạm
        this._queuePending('_pendingToB', frame);
      }
      return;
    }

    if (tags.includes(TAG_B)) {
      // B → A
      const a = this._getA();
      if (a) {
        try { a.send(frame); } catch { this._queuePending('_pendingToA', frame); }
      } else {
        this._queuePending('_pendingToA', frame);
      }
      return;
    }
  }

  webSocketClose(ws, code, reason) {
    const tags = this._tags(ws);
    const side = tags.includes(TAG_A) ? 'A' : tags.includes(TAG_B) ? 'B' : '?';

    // [FIX-2] Phân biệt "bị thay thế bởi reconnect" với "rớt kết nối thật".
    // Tại thời điểm callback này chạy, nếu đã có một socket MỚI được đăng ký
    // cho cùng tag (do _acceptA/_acceptB gọi acceptWebSocket trước khi close
    // handshake của socket cũ hoàn tất), thì _getA()/_getB() sẽ trả về socket
    // mới đó (khác với `ws` đang đóng) → đây chỉ là dọn dẹp socket cũ, KHÔNG
    // phải mất kết nối thật, nên bỏ qua, không báo cho phía đối diện.
    if (tags.includes(TAG_A)) {
      const currentA = this._getA();
      if (currentA && currentA !== ws) {
        console.log(`[DO] Client A (socket cũ) đóng (${code}) — đã có A mới, bỏ qua`);
        return;
      }
      console.log(`[DO] Client A ngắt kết nối thật (${code})`);
      // A đóng thật → dừng alarm, báo B
      this.state.storage.deleteAlarm().catch(() => {});
      const b = this._getB();
      if (b) try { b.close(1001, 'Tunnel disconnected'); } catch {}
      return;
    }

    if (tags.includes(TAG_B)) {
      const currentB = this._getB();
      if (currentB && currentB !== ws) {
        console.log(`[DO] Client B (socket cũ) đóng (${code}) — đã có B mới, bỏ qua`);
        return;
      }
      console.log(`[DO] Client B ngắt kết nối thật (${code})`);
      // B đóng thật → báo A (text frame nhỏ thay vì binary custom protocol)
      const a = this._getA();
      if (a) try { a.send('CLIENT_DISCONNECTED'); } catch {}
      return;
    }

    console.log(`[DO] Client ${side} ngắt kết nối (${code})`);
  }

  webSocketError(ws, error) {
    const tags = this._tags(ws);
    const side = tags.includes(TAG_A) ? 'A' : tags.includes(TAG_B) ? 'B' : '?';
    console.error(`[DO] Client ${side} lỗi:`, error?.message ?? error);
    // Không gọi ws.close() trên WS đã lỗi — chỉ dọn dẹp phía kia.
    // webSocketClose() bên dưới đã tự có logic [FIX-2] để tránh báo nhầm.
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
    this._pendingToA = [];
    this._pendingToB = [];
    this._pendingToABytes = 0;
    this._pendingToBBytes = 0;
  }
}

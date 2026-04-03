/**
 * Lite HTTP Tunnel — Cloudflare Workers entry point
 *
 * Mỗi request đến đều được route tới đúng TunnelHost Durable Object
 * tương ứng với hostname. DO instance đó giữ toàn bộ state của tunnel
 * (WebSocket connection, pending requests) — không cần biến global nào.
 *
 * Sơ đồ routing:
 *
 *   Internet
 *     │
 *     ▼
 *   CF Worker  ──idFromName(host)──►  TunnelHost DO  (1 instance / host)
 *                                          │
 *                    ┌─────────────────────┼──────────────────────┐
 *                    ▼                     ▼                      ▼
 *             /$web_tunnel          /tunnel_jwt_generator   HTTP / WS
 *          (tunnel client WS)       (lấy JWT token)      (proxy qua tunnel)
 */

export { TunnelHost } from './TunnelHost.js';

export default {
  /**
   * @param {Request} request
   * @param {{ TUNNEL_HOST: DurableObjectNamespace, [key: string]: string }} env
   */
  async fetch(request, env) {
    const host = request.headers.get('host') || new URL(request.url).hostname;

    // Mỗi hostname → một TunnelHost Durable Object riêng biệt
    const id   = env.TUNNEL_HOST.idFromName(host);
    const stub = env.TUNNEL_HOST.get(id);

    return stub.fetch(request);
  },
};

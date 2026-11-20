# CoWatch V6 — modular engineering build

CoWatch V6 is a two-person watch-party web application. The current build contains a same-origin static/WebSocket server, native WebRTC signaling and data channels, chunked P2P transfer, IndexedDB persistence, a Range-capable Service Worker media endpoint, host-authoritative synchronization, chat, reactions, direct video/HLS URLs, external website/YouTube embed mode, and a screen-sharing path.

## Run locally

Requirements: **Node.js 20 or newer**. No `npm install` is required; the server uses Node built-ins.

```bash
npm start
```

Open `http://localhost:8787` on the first device, create a room, and open the invite link on the second device. Both devices must be able to reach the same server. For a real two-device test, expose the app over HTTPS; browsers require a secure context for Service Workers, WebRTC media capture, and several storage features. `localhost` is treated as secure for local development.

Optional environment variables:

- `PORT=8787` — HTTP and WebSocket port.
- `ALLOWED_ORIGINS=https://app.example.com,https://www.example.com` — comma-separated exact browser origins allowed to connect. When omitted, the server accepts only an Origin whose host matches the request Host header.

Run tests:

```bash
npm test
```

## Architecture

- `server/signaling.js`: Node HTTP static server plus a small WebSocket room/signaling relay. It does not relay media bytes.
- `src/rooms/signaling.js`: browser signaling client.
- `src/webrtc/peer.js`: RTCPeerConnection and ordered reliable DataChannel.
- `src/transfer/transfer.js`: chunk metadata, binary payloads, per-chunk SHA-256 validation, ACKs, simple seek-priority queue, and resume metadata.
- `src/storage/db.js`: IndexedDB persistence.
- `cowatch-sw.js`: virtual `/media/{transferId}` endpoint with GET, HEAD, single byte-range support, 206 and 416 behavior. Missing chunks trigger a request from the active receiving page to the P2P peer.
- `src/sync/sync.js`: host-authoritative playback state, guest control requests, RTT estimate, and bounded drift correction.
- `src/media/compatibility.js`: best-effort `canPlayType` and `MediaCapabilities` probing; not a full media parser.

## What is implemented

- Two-person rooms and invite links.
- Same-origin WebSocket signaling with room capacity enforcement and basic message rate limiting.
- WebRTC direct peer connection using public STUN servers.
- Reliable ordered DataChannel file transfer in 256 KiB chunks.
- SHA-256 verification per chunk before IndexedDB persistence.
- ACK-based sender pacing and bounded DataChannel buffering.
- Resume metadata keyed by a fingerprint derived from file name, size, modification time, and a sample hash.
- Service Worker Range endpoint that reads chunks from IndexedDB without assembling the whole movie in RAM.
- Missing chunk requests from the Service Worker to the active receiving page and then over P2P.
- Direct HTTPS video URLs and HLS.js playback where the browser/source permit it.
- Host-controlled play/pause/seek/rate with guest control requests.
- Chat, quick emoji reactions, external iframe mode, YouTube embed mode, and screen-capture/renegotiation path.
- Arabic RTL responsive UI.

## Important limitations — not production-ready yet

- The sender uses sequential chunk delivery plus a basic priority queue. It is not yet an adaptive multi-channel scheduler.
- The Service Worker can wait for missing chunks and request them from the connected peer, but P2P cannot be guaranteed to continue after the receiving page is suspended or closed. Mobile browsers may throttle background pages.
- MP4 atom parsing and index-first tail metadata scheduling are **not implemented**. Files with `moov` at the end may not start quickly; fast-start MP4 files are the best candidates for early playback.
- The receiver verifies every new chunk independently, but a final whole-file digest comparison is not yet implemented. Existing resumed chunks are not re-hashed against the newly selected source file before being trusted.
- OPFS is not wired in; IndexedDB is the current chunk store. Quota is checked before receiving, but browser quota behavior varies.
- TURN is not configured. Public STUN is included only. Restrictive NATs require a production TURN service and short-lived credentials from a trusted backend. Do not place static TURN secrets in client JavaScript.
- The signaling service has room capacity and basic rate limiting, but no durable user authentication, signed invite tokens, distributed room state, or advanced abuse protection. Use TLS and an appropriate reverse proxy before public deployment.
- Screen sharing has a renegotiation and remote-track path, but it has not been validated across physical Android devices and desktop browsers.
- HLS.js and YouTube embed resources are loaded from public CDNs. DASH, full YouTube IFrame API controls/sync, subtitle/audio track selection, transcoding backend, and picture-in-picture controls are not implemented.
- External sites may block iframe embedding through CSP or X-Frame-Options. Arbitrary third-party player synchronization is not possible without a permitted API/integration.
- No 100MB/500MB/1GB/2GB/4GB end-to-end transfer, 2GB progressive seek, Safari, Android browser, or two-physical-device WebRTC test has been completed in this environment.

## Production deployment

1. Put the app behind HTTPS. WebSocket upgrades must be proxied to the same Node server. If signaling is hosted separately, set the `content` value of `<meta name="cowatch-signal">` in `index.html` to its `wss://` URL.
2. Set `ALLOWED_ORIGINS` to the exact frontend origin(s).
3. Deploy and configure a TURN service; add a backend endpoint that mints short-lived TURN credentials and supply those ICE servers to authorized sessions.
4. Add security headers/CSP appropriate to your deployment domains, logging, monitoring, connection cleanup, and stronger room authentication.
5. Run real two-device tests, large-file tests, network-loss/reconnect tests, storage quota tests, and media compatibility tests before calling the product production-ready.

## Byte preservation vs playback compatibility

The transfer layer does not transcode or recompress the selected file. It transfers the selected file's bytes in chunks and verifies each received chunk with SHA-256. This does not guarantee that a particular browser can decode the file: the container and codecs inside it must still be supported by that browser/device.

# Mojo Gate RFC Compliance Matrix

This document details the normative compliance of **Mojo Gate** (`mojo-gate`) with official IETF Request for Comments (RFC) standards governing HTTP/1.1, message framing, WebSocket tunneling, and security boundaries.

---

## 1. Standards Overview & Lifecycle Status

| RFC Number | Title | Status | Scope in Mojo Gate |
| :--- | :--- | :--- | :--- |
| **RFC 9110** | HTTP Semantics | Internet Standard (STD 97) | Method tokens (`tchar`), cache control, content negotiation, status codes. |
| **RFC 9112** | HTTP/1.1 Message Syntax and Routing | Internet Standard (STD 99) | Request line parsing, URI limits, Host validation, smuggling defenses. |
| **RFC 6455** | The WebSocket Protocol | Proposed Standard | Protocol upgrade handshakes (`101 Switching Protocols`), tunneling. |
| **RFC 2119 / 8174** | Key words for use in RFCs to Indicate Requirement Levels | Best Current Practice (BCP 14) | Normative enforcement (`MUST`, `MUST NOT`, `SHOULD`). |

---

## 2. Normative Conformance Matrix (RFC 9110 & RFC 9112)

### RFC 9112 §3 & §5: Request Line & Headers
- **`MUST` validate method tokens**: Mojo Gate enforces RFC 9110 §5.6.2 `tchar` validation on request methods, rejecting illegal characters with `400 Bad Request`.
- **`MUST` validate target URI length**: Targets exceeding `4096` bytes are rejected with `414 URI Too Long`.
- **`MUST` enforce HTTP version**: Only `HTTP/1.1` and `HTTP/1.0` are accepted; other versions receive `400 Bad Request`.
- **`MUST` require Host header in HTTP/1.1**: HTTP/1.1 requests missing a `Host` header or containing duplicate `Host` headers are rejected with `400 Bad Request` (RFC 9112 §7.1).
- **`MUST NOT` allow whitespace before colon**: Header lines with whitespace between field name and colon (RFC 7230 / RFC 9112 header injection vector) are strictly rejected with `400 Bad Request`.

### Request Smuggling & Dual Framing Defenses
- **`MUST NOT` permit conflicting framing**: Upstream responses and client requests supplying both `Content-Length` and `Transfer-Encoding: chunked` are rejected with `502 Bad Gateway` to prevent request smuggling / desynchronization.
- **`MUST` percent-decode path after query separation**: Query strings are separated on `?` *before* percent-decoding and path normalization (RFC 3986) to prevent query delimiter confusion or traversal bypasses.

---

## 3. Caching & Invalidation Compliance (RFC 9110 §7 & §8)

- **Safe Methods Only**: Only `GET` and `HEAD` requests are candidates for in-memory micro-caching.
- **Write Invalidation**: Any write request (`POST`, `PUT`, `DELETE`, `PATCH`) automatically triggers an immediate cache flush (`flush_cache()`).
- **Cache-Control Directives**: Responses specifying `no-store`, `private`, `no-cache`, `max-age=0`, `s-maxage=0`, `Vary: *`, or `Content-Range` are strictly excluded from being cached (`capture = False`).
- **Dynamic Date Generation**: Cached responses are replayed with dynamically regenerated RFC 7231 GMT `Date` headers using cached second-tick timestamps.

---

## 4. WebSocket & Protocol Tunneling (RFC 6455)

- **Deferred Upgrade Activation**: Client connection requests with `Upgrade` (e.g. WebSockets) or `CONNECT` remain in `C_WAITING` state until upstream responds with strictly `101 Switching Protocols`.
- **Upstream Connection Headers**: Upgrade handshakes correctly propagate `Connection: Upgrade` upstream to satisfy ASGI/WebSocket server requirements (Uvicorn, Starlette).
- **Full-Duplex Tunneling**: Upon successful `101` handshake or non-upgrade tunnel initiation, proxy transitions both client and upstream descriptors into a transparent raw byte pipe (`C_TUNNEL`).

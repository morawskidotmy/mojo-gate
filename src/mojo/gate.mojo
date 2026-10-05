"""Mojo Gate: High-performance reverse proxy & API gateway in Mojo.

Provides:
- Non-blocking epoll event loop with non-blocking sockets.
- Zero-overhead in-memory response caching for safe GET/HEAD requests with
  RFC 7231 Date header regeneration.
- Transparent upstream proxying with connection reuse and client keep-alive.
- Per-IP sliding-window rate limiting with standard 429 Too Many Requests response.
- Tunneling for WebSockets (Upgrade), SSE, chunked uploads, Expect 100-continue.
- Instant cache invalidation on write requests (POST/PUT/DELETE/PATCH), or on-demand
  cache purge via endpoint (`/_mojo_gate/purge`).
- Batched cache-hit analytics reporting to upstream.
"""

from std.collections import Dict, List
from std.ffi import external_call, c_int, c_char, c_size_t
from std.memory.pointer import Pointer
from std.sys.info import size_of
from std.sys import argv
from std.time import monotonic

comptime AF_INET = 2
comptime SOCK_STREAM = 1
comptime SOL_SOCKET = 1
comptime SO_REUSEADDR = 2
comptime SO_ERROR = 4
comptime SO_RCVTIMEO = 20
comptime SO_SNDTIMEO = 21
comptime IPPROTO_TCP = 6
comptime TCP_NODELAY = 1
comptime FIONBIO = 0x5421
comptime EAGAIN = 11
comptime EINPROGRESS = 115

comptime EPOLLIN = 1
comptime EPOLLOUT = 4
comptime EPOLLERR = 8
comptime EPOLLHUP = 16
comptime EPOLLRDHUP = 8192
comptime EPOLL_CTL_ADD = 1
comptime EPOLL_CTL_DEL = 2
comptime EPOLL_CTL_MOD = 3

comptime MAX_HEADER_BYTES = 65536
comptime MAX_TARGET_BYTES = 4096
comptime MAX_INBUF_BYTES = 16777216  # 16 MB max buffered client bytes
comptime MAX_BUCKETS = 65536
comptime MAX_HITS = 2048

# Client connection states
comptime C_IDLE = 0      # reading / parsing requests
comptime C_WAITING = 1   # request forwarded to upstream; later ones queue in inbuf
comptime C_TUNNEL = 2    # raw byte pipe (Upgrade, chunked uploads, Expect, CONNECT)

# Upstream connection states
comptime U_CONNECTING = 10
comptime U_OPEN = 11


struct SockAddrIn(TrivialRegisterPassable):
    var sin_family: UInt16
    var sin_port: UInt16
    var sin_addr: UInt32
    var sin_zero0: UInt64

    def __init__(out self, port: Int, addr: UInt32 = 0x0100007F):
        self.sin_family = 2
        var p = UInt16(port)
        self.sin_port = ((p & 0xFF) << 8) | ((p >> 8) & 0xFF)
        self.sin_addr = addr
        self.sin_zero0 = 0


struct EpollEvent(TrivialRegisterPassable):
    var events: UInt32
    var fd: Int32
    var pad: UInt32

    def __init__(out self, events: UInt32, fd: Int32):
        self.events = events
        self.fd = fd
        self.pad = 0


struct TimeVal(TrivialRegisterPassable):
    var tv_sec: Int64
    var tv_usec: Int64

    def __init__(out self, sec: Int64):
        self.tv_sec = sec
        self.tv_usec = 0


struct RateRule(Copyable, Movable, ImplicitlyCopyable):
    var prefix: String
    var limit: Int
    var window_s: Int

    def __init__(out self, prefix: String, limit: Int, window_s: Int):
        self.prefix = prefix
        self.limit = limit
        self.window_s = window_s


struct Conn(Copyable, Movable):
    var active: Bool
    var upstream: Bool
    var peer: Int            # client <-> upstream fd link, -1 if none
    var state: Int
    var inbuf: List[UInt8]   # client: unparsed request bytes
    var out: List[UInt8]     # bytes still to send to this fd
    var out_off: Int
    var want_out: Bool       # EPOLLOUT currently registered
    var keep_alive: Bool     # client: keep the connection after this response
    var close_after: Bool    # client: close once `out` is drained
    # upstream-only response bookkeeping
    var resp: List[UInt8]    # raw response bytes (until headers parsed / while capturing)
    var hdr_done: Bool
    var capture: Bool        # store the full response in the cache at EOF
    var cache_key: String
    var head_req: Bool
    var cl: Int              # content-length of the response (-1 unknown)
    var chunked: Bool
    var body_seen: Int
    var client_ip: String
    var last_active: Int

    def __init__(out self):
        self.active = False
        self.upstream = False
        self.peer = -1
        self.state = C_IDLE
        self.inbuf = List[UInt8]()
        self.out = List[UInt8]()
        self.out_off = 0
        self.want_out = False
        self.keep_alive = True
        self.close_after = False
        self.resp = List[UInt8]()
        self.hdr_done = False
        self.capture = False
        self.cache_key = String("")
        self.head_req = False
        self.cl = -1
        self.chunked = False
        self.body_seen = 0
        self.client_ip = String("127.0.0.1")
        self.last_active = now_s()


struct Entry(Copyable, Movable):
    var status_line: List[UInt8]  # "HTTP/1.1 200 OK\r\n"
    var rest_close: List[UInt8]   # headers after `date`, incl. "connection: close" and final CRLF
    var rest_keep: List[UInt8]    # same without the connection line
    var body: List[UInt8]
    var born: Int                 # monotonic seconds
    var size: Int

    def __init__(out self, var status_line: List[UInt8], var rest_close: List[UInt8],
                 var rest_keep: List[UInt8], var body: List[UInt8], born: Int):
        self.size = len(status_line) + len(rest_close) + len(rest_keep) + len(body)
        self.status_line = status_line^
        self.rest_close = rest_close^
        self.rest_keep = rest_keep^
        self.body = body^
        self.born = born


struct Request(Copyable, Movable):
    var ok: Bool
    var bad_status: Int       # 400, 413, 501 if not ok
    var method: String
    var target: String        # raw request-target
    var raw_path: String
    var raw_query: String
    var version: String
    var head_len: Int         # bytes of request line + headers incl. CRLFCRLF
    var body_len: Int
    var chunked: Bool
    var upgrade: Bool
    var expect: Bool
    var conn_close: Bool
    var conn_keep: Bool
    var xff: String           # all X-Forwarded-For values joined with ", "
    var has_xff: Bool
    var user_agent: String    # raw bytes as latin-1 code points
    var accept_gzip: Bool
    var has_origin: Bool
    var host: String
    var xfp: String
    var xfh: String
    var uncacheable_hdr: Bool  # Cookie / Authorization / Range / If-* present
    var non_ascii: Bool
    var token_matched: Bool    # X-Mojo-Gate-Token matched internal secret
    var lines: List[String]    # header lines (raw), to rebuild the upstream request

    def __init__(out self):
        self.ok = False
        self.bad_status = 400
        self.method = String("")
        self.target = String("")
        self.raw_path = String("")
        self.raw_query = String("")
        self.version = String("")
        self.head_len = 0
        self.body_len = 0
        self.chunked = False
        self.upgrade = False
        self.expect = False
        self.conn_close = False
        self.conn_keep = False
        self.xff = String("")
        self.has_xff = False
        self.user_agent = String("")
        self.accept_gzip = False
        self.has_origin = False
        self.host = String("")
        self.xfp = String("")
        self.xfh = String("")
        self.uncacheable_hdr = False
        self.non_ascii = False
        self.token_matched = False
        self.lines = List[String]()


# ─── small helpers ─────────────────────────────────────────────────────────


def format_ipv4(addr: UInt32) -> String:
    var a = Int(addr & 0xFF)
    var b = Int((addr >> 8) & 0xFF)
    var c = Int((addr >> 16) & 0xFF)
    var d = Int((addr >> 24) & 0xFF)
    return String(a) + "." + String(b) + "." + String(c) + "." + String(d)


def is_valid_tchar(b: UInt8) -> Bool:
    var c = Int(b)
    # ALPHA (A-Z, a-z)
    if (c >= 65 and c <= 90) or (c >= 97 and c <= 122):
        return True
    # DIGIT (0-9)
    if c >= 48 and c <= 57:
        return True
    # RFC 9110 symbols: ! # $ % & ' * + - . ^ _ ` | ~
    return (c == 33 or c == 35 or c == 36 or c == 37 or c == 38 or c == 39
            or c == 42 or c == 43 or c == 45 or c == 46 or c == 94 or c == 95
            or c == 96 or c == 124 or c == 126)


def normalize_path(p: String) -> String:
    """RFC 3986 path normalization removing dot segments (. / ..) and multiple slashes."""
    if p.byte_length() == 0:
        return String("/")
    var parts = p.split("/")
    var stack = List[String]()
    for i in range(len(parts)):
        var seg = String(parts[i])
        if seg.byte_length() == 0 or seg == ".":
            continue
        if seg == "..":
            if len(stack) > 0:
                _ = stack.pop()
        else:
            stack.append(seg)
    var out = String("")
    for i in range(len(stack)):
        out += "/" + stack[i]
    if out.byte_length() == 0:
        return String("/")
    return out


def errno() -> Int:
    var p = external_call["__errno_location", Pointer[Int32, MutUntrackedOrigin]]()
    return Int(p[])


def now_s() -> Int:
    return Int(monotonic() // 1_000_000_000)


def append_str(mut buf: List[UInt8], s: String):
    for b in s.as_bytes():
        buf.append(b)


def find_crlfcrlf(buf: List[UInt8], start: Int) -> Int:
    """Index just past the first CRLFCRLF at or after `start`, or -1."""
    var i = start if start > 0 else 0
    var n = len(buf)
    while i + 3 < n:
        if buf[i] == 13 and buf[i + 1] == 10 and buf[i + 2] == 13 and buf[i + 3] == 10:
            return i + 4
        i += 1
    return -1


def latin1(buf: List[UInt8], start: Int, end: Int) -> String:
    """Bytes -> String, one code point per byte."""
    var s = String("")
    for i in range(start, end):
        s += chr(Int(buf[i]))
    return s


def lower_ascii(s: String) -> String:
    var out = String("")
    for b in s.as_bytes():
        var c = Int(b)
        if c >= 65 and c <= 90:
            out += chr(c + 32)
        else:
            out += chr(c)
    return out


def strip_ws(s: String) -> String:
    var bytes = s.as_bytes()
    var a = 0
    var b = len(bytes)
    while a < b and (bytes[a] == 32 or bytes[a] == 9):
        a += 1
    while b > a and (bytes[b - 1] == 32 or bytes[b - 1] == 9):
        b -= 1
    return String(s[byte=a:b])


def hex_val(b: Int) -> Int:
    if b >= 48 and b <= 57:
        return b - 48
    if b >= 65 and b <= 70:
        return b - 55
    if b >= 97 and b <= 102:
        return b - 87
    return -1


def percent_decode_bytes(s: String) -> List[UInt8]:
    """%XX-decode (no '+' handling), like urllib.parse.unquote on the path."""
    var out = List[UInt8]()
    var bytes = s.as_bytes()
    var n = len(bytes)
    var i = 0
    while i < n:
        var b = Int(bytes[i])
        if b == 37 and i + 2 < n:
            var h1 = hex_val(Int(bytes[i + 1]))
            var h2 = hex_val(Int(bytes[i + 2]))
            if h1 >= 0 and h2 >= 0:
                out.append(UInt8(h1 * 16 + h2))
                i += 3
                continue
        out.append(UInt8(b))
        i += 1
    return out^


def bytes_startswith(buf: List[UInt8], prefix: String) -> Bool:
    var p = prefix.as_bytes()
    if len(buf) < len(p):
        return False
    for i in range(len(p)):
        if buf[i] != p[i]:
            return False
    return True


def is_digits(s: String) -> Bool:
    if s.byte_length() == 0:
        return False
    for b in s.as_bytes():
        if b < 48 or b > 57:
            return False
    return True


def parse_int(s: String) -> Int:
    var v = 0
    if s.byte_length() == 0:
        return -1
    for b in s.as_bytes():
        if b < 48 or b > 57:
            return -1
        v = v * 10 + Int(b) - 48
    return v


def parse_ipv4(s: String) -> UInt32:
    if s == "0.0.0.0" or s == "*":
        return 0
    var parts = s.split(".")
    if len(parts) == 4:
        var a = parse_int(String(parts[0]))
        var b = parse_int(String(parts[1]))
        var c = parse_int(String(parts[2]))
        var d = parse_int(String(parts[3]))
        if a >= 0 and a <= 255 and b >= 0 and b <= 255 and c >= 0 and c <= 255 and d >= 0 and d <= 255:
            return UInt32(a) | (UInt32(b) << 8) | (UInt32(c) << 16) | (UInt32(d) << 24)
    return 0x0100007F  # default to 127.0.0.1


def json_str(s: String) -> String:
    """JSON string literal; every code point above 0x7E as \\u00XX (latin-1)."""
    var out = String('"')
    for cp in s.codepoints():
        var c = Int(cp)
        if c == 34:
            out += '\\"'
        elif c == 92:
            out += "\\\\"
        elif c < 32 or c > 126:
            var h = String("0123456789abcdef")
            out += "\\u"
            out += String(h[byte=(c >> 12) & 15:((c >> 12) & 15) + 1])
            out += String(h[byte=(c >> 8) & 15:((c >> 8) & 15) + 1])
            out += String(h[byte=(c >> 4) & 15:((c >> 4) & 15) + 1])
            out += String(h[byte=c & 15:(c & 15) + 1])
        else:
            out += chr(c)
    out += '"'
    return out


def two(n: Int) -> String:
    return String("0") + String(n) if n < 10 else String(n)


def http_date() -> String:
    """RFC 7231 date for the current second."""
    var t = external_call["time", Int](Int(0))
    var days = t // 86400
    var secs = t % 86400
    var weekday = (days + 4) % 7  # 1970-01-01 was a Thursday
    # civil_from_days (Howard Hinnant)
    var z = days + 719468
    var era = z // 146097
    var doe = z - era * 146097
    var yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    var y = yoe + era * 400
    var doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    var mp = (5 * doy + 2) // 153
    var d = doy - (153 * mp + 2) // 5 + 1
    var m = mp + 3 if mp < 10 else mp - 9
    if m <= 2:
        y += 1
    var wd = String("SunMonTueWedThuFriSat")
    var mn = String("JanFebMarAprMayJunJulAugSepOctNovDec")
    return (String(wd[byte=weekday * 3:weekday * 3 + 3]) + ", " + two(d) + " "
            + String(mn[byte=(m - 1) * 3:(m - 1) * 3 + 3]) + " " + String(y) + " "
            + two(secs // 3600) + ":" + two((secs % 3600) // 60) + ":" + two(secs % 60)
            + " GMT")


# ─── request parsing ───────────────────────────────────────────────────────


def parse_request(buf: List[UInt8], internal_token: String = "") -> Request:
    """Parse the request head at the start of `buf` (caller checked CRLFCRLF)."""
    var r = Request()
    var end = find_crlfcrlf(buf, 0)
    if end < 0:
        return r^
    r.head_len = end
    # request line
    var i = 0
    while i + 1 < end and not (buf[i] == 13 and buf[i + 1] == 10):
        i += 1
    var line = latin1(buf, 0, i)
    var parts = line.split(" ")
    if len(parts) != 3:
        r.ok = False
        r.bad_status = 400
        return r^
    r.method = String(parts[0])
    r.target = String(parts[1])
    r.version = String(parts[2])

    # Validate HTTP method tchar (RFC 9110 §5.6.2)
    if r.method.byte_length() == 0:
        r.ok = False
        r.bad_status = 400
        return r^
    for b in r.method.as_bytes():
        if not is_valid_tchar(b):
            r.ok = False
            r.bad_status = 400
            return r^

    # Validate target length (RFC 9112 §3)
    if r.target.byte_length() > MAX_TARGET_BYTES:
        r.ok = False
        r.bad_status = 414  # URI Too Long
        return r^

    # Validate HTTP version (RFC 9112 §2.3)
    if r.version != "HTTP/1.1" and r.version != "HTTP/1.0":
        r.ok = False
        r.bad_status = 400
        return r^

    for b in r.target.as_bytes():
        if b > 126 or b < 33:
            r.non_ascii = True
    var q = r.target.find("?")
    var raw_p: String
    var raw_q = String("")
    if q >= 0:
        raw_p = String(r.target[byte=0:q])
        raw_q = String(r.target[byte=q + 1:r.target.byte_length()])
    else:
        raw_p = r.target
    var p_bytes = percent_decode_bytes(raw_p)
    r.raw_path = normalize_path(latin1(p_bytes, 0, len(p_bytes)))
    r.raw_query = raw_q
    var pos = i + 2
    var first_xff = True
    var have_ae = False
    var have_cl = False
    var body_len = 0
    while pos + 1 < end:
        var e = pos
        while e + 1 < end and not (buf[e] == 13 and buf[e + 1] == 10):
            e += 1
        if e == pos:
            break
        # Reject control characters (CRLF injection / smuggling)
        for k in range(pos, e):
            var b = Int(buf[k])
            if b < 32 and b != 9:
                r.ok = False
                r.bad_status = 400
                return r^
            if b > 126:
                r.non_ascii = True
        # Disallow leading whitespace in header lines (RFC 9112 §5.2)
        if buf[pos] == 32 or buf[pos] == 9:
            r.ok = False
            r.bad_status = 400
            return r^
        var hl = latin1(buf, pos, e)
        var colon = hl.find(":")
        if colon <= 0 or (colon > 0 and (hl.as_bytes()[colon - 1] == 32 or hl.as_bytes()[colon - 1] == 9)):
            # Disallow header names with whitespace before colon (RFC 7230 §3.2.4)
            r.ok = False
            r.bad_status = 400
            return r^
        # Validate header name characters (RFC 9110 token)
        for byte_idx in range(colon):
            if not is_valid_tchar(hl.as_bytes()[byte_idx]):
                r.ok = False
                r.bad_status = 400
                return r^
        r.lines.append(hl)
        var name = lower_ascii(String(hl[byte=0:colon]))
        var value = strip_ws(String(hl[byte=colon + 1:hl.byte_length()]))
        if name == "content-length":
            if have_cl:
                # Conflicting duplicate Content-Length headers
                r.ok = False
                r.bad_status = 400
                return r^
            if r.chunked:
                # Both Transfer-Encoding and Content-Length present (smuggling attack)
                r.ok = False
                r.bad_status = 400
                return r^
            var v = parse_int(value)
            if v < 0:
                r.ok = False
                r.bad_status = 400
                return r^
            if v > MAX_INBUF_BYTES:
                r.ok = False
                r.bad_status = 413
                return r^
            body_len = v
            have_cl = True
        elif name == "transfer-encoding":
            if have_cl:
                # Both Transfer-Encoding and Content-Length present (smuggling attack)
                r.ok = False
                r.bad_status = 400
                return r^
            var lv = lower_ascii(value)
            if lv == "chunked":
                r.chunked = True
            else:
                r.ok = False
                r.bad_status = 501
                return r^
        elif name == "upgrade":
            r.upgrade = True
        elif name == "expect":
            r.expect = True
        elif name == "connection":
            var lv = lower_ascii(value)
            if lv.find("close") >= 0:
                r.conn_close = True
            if lv.find("keep-alive") >= 0:
                r.conn_keep = True
            if lv.find("upgrade") >= 0:
                r.upgrade = True
        elif name == "x-forwarded-for":
            if first_xff:
                r.xff = value
                first_xff = False
            else:
                r.xff += ", " + value
            r.has_xff = True
        elif name == "user-agent":
            if r.user_agent.byte_length() == 0:
                r.user_agent = value
        elif name == "accept-encoding":
            if not have_ae:
                r.accept_gzip = value.find("gzip") >= 0
                have_ae = True
        elif name == "origin":
            r.has_origin = True
        elif name == "host":
            if r.host.byte_length() > 0:
                # Reject duplicate Host header (RFC 9112 §7.1)
                r.ok = False
                r.bad_status = 400
                return r^
            r.host = lower_ascii(value)
        elif name == "x-forwarded-proto":
            r.xfp = value
        elif name == "x-forwarded-host":
            r.xfh = value
        elif name == "x-mojo-gate-token":
            if internal_token.byte_length() > 0 and value == internal_token:
                r.token_matched = True
        elif (name == "cookie" or name == "authorization" or name == "range"
              or name == "proxy-authorization" or name == "x-api-key"
              or name == "api-key" or name == "x-auth-token" or name == "x-session-id"
              or name == "if-none-match" or name == "if-modified-since"
              or name == "if-range" or name == "if-match"
              or name == "if-unmodified-since"):
            r.uncacheable_hdr = True
        pos = e + 2
    if r.version == "HTTP/1.1" and r.host.byte_length() == 0:
        # HTTP/1.1 requires a Host header (RFC 9112 §7.1)
        r.ok = False
        r.bad_status = 400
        return r^
    r.body_len = body_len
    r.ok = True
    return r^


def client_ip(r: Request) -> String:
    """Client IP derived from X-Forwarded-For (matching ProxyHeadersMiddleware)."""
    if not r.has_xff:
        return String("127.0.0.1")
    var hosts = List[String]()
    for part in r.xff.split(","):
        hosts.append(strip_ws(String(part)))
    var idx = len(hosts) - 1
    while idx >= 0:
        var h = parse_host(hosts[idx])
        if h != "127.0.0.1":
            return h
        idx -= 1
    return parse_host(hosts[0])


def parse_host(value: String) -> String:
    """Host part from host[:port]."""
    if value.startswith("["):
        var close = value.find("]")
        if close == -1:
            return value
        var host = String(value[byte=1:close])
        var rem = String(value[byte=close + 1:value.byte_length()])
        if rem.byte_length() == 0 or rem.startswith(":"):
            return host
        return value
    if value.count(":") == 1:
        var c = value.find(":")
        if is_digits(String(value[byte=c + 1:value.byte_length()])):
            return String(value[byte=0:c])
        return value
    return value


def cache_key(r: Request, is_loopback: Bool) -> String:
    var base = r.method + "\x1f" + r.raw_path
    if r.raw_query.byte_length() > 0:
        base += "?" + r.raw_query
    base += "\x1fg" + ("1" if r.accept_gzip else "0")
    base += "\x1fo" + ("1" if r.has_origin else "0")
    base += "\x1fh" + r.host
    if is_loopback:
        base += "\x1fp" + r.xfp + "\x1fx" + r.xfh
    return base


def upstream_request(r: Request, buf: List[UInt8], client_ip: String) -> List[UInt8]:
    """The request as sent to upstream: same head with `Connection: close` and client IP."""
    var out = List[UInt8]()
    append_str(out, r.method + " " + r.target + " " + r.version + "\r\n")
    var forwarded_written = False
    var proto_written = False
    var is_loopback = (client_ip == "127.0.0.1" or client_ip == "::1")
    for i in range(len(r.lines)):
        var l = r.lines[i]
        var colon = l.find(":")
        if colon > 0:
            var name = lower_ascii(String(l[byte=0:colon]))
            if (name == "connection" or name == "keep-alive" or name == "proxy-connection"
                or name == "x-mojo-gate-token"):
                continue
            if name == "x-forwarded-for":
                append_str(out, "X-Forwarded-For: " + r.xff + ", " + client_ip + "\r\n")
                forwarded_written = True
                continue
            if name == "x-forwarded-proto":
                if not is_loopback:
                    continue  # untrusted client cannot spoof scheme
                proto_written = True
        # header lines are latin-1 decoded; re-encode byte-for-byte
        for cp in l.codepoints():
            out.append(UInt8(Int(cp)))
        append_str(out, "\r\n")
    if not forwarded_written and client_ip.byte_length() > 0:
        append_str(out, "X-Forwarded-For: " + client_ip + "\r\n")
    if not proto_written:
        append_str(out, "X-Forwarded-Proto: http\r\n")
    if r.upgrade:
        append_str(out, "Connection: Upgrade\r\n\r\n")
    else:
        append_str(out, "Connection: close\r\n\r\n")
    for i in range(r.head_len, r.head_len + r.body_len):
        out.append(buf[i])
    return out^


# ─── the proxy ─────────────────────────────────────────────────────────────


struct Proxy:
    var epfd: c_int
    var listen_fd: c_int
    var upstream_host_ip: UInt32
    var upstream_port: Int
    var cache_ttl_s: Int
    var cache_max_bytes: Int
    var entry_max_bytes: Int
    var rate_limit: Bool
    var rate_rules: List[RateRule]
    var rate_limit_msg: String
    var no_cache_prefixes: List[String]
    var analytics_endpoint: String
    var purge_endpoint: String
    var internal_token: String
    var conns: List[Conn]
    var cache: Dict[String, Entry]
    var cache_bytes: Int
    var buckets: Dict[String, List[Int]]  # "ip|prefix" -> request times (ms)
    var hits: List[String]                # analytics JSON items to report
    var tmp: List[UInt8]

    def __init__(out self, upstream_host_ip: UInt32, upstream_port: Int,
                 cache_ttl_s: Int, cache_max_bytes: Int, entry_max_bytes: Int,
                 rate_limit: Bool, var rate_rules: List[RateRule],
                 rate_limit_msg: String, var no_cache_prefixes: List[String],
                 analytics_endpoint: String, purge_endpoint: String,
                 internal_token: String):
        self.epfd = 0
        self.listen_fd = 0
        self.upstream_host_ip = upstream_host_ip
        self.upstream_port = upstream_port
        self.cache_ttl_s = cache_ttl_s
        self.cache_max_bytes = cache_max_bytes
        self.entry_max_bytes = entry_max_bytes
        self.rate_limit = rate_limit
        self.rate_rules = rate_rules^
        self.rate_limit_msg = rate_limit_msg
        self.no_cache_prefixes = no_cache_prefixes^
        self.analytics_endpoint = analytics_endpoint
        self.purge_endpoint = purge_endpoint
        self.internal_token = internal_token
        self.conns = List[Conn]()
        self.cache = Dict[String, Entry]()
        self.cache_bytes = 0
        self.buckets = Dict[String, List[Int]]()
        self.hits = List[String]()
        self.tmp = List[UInt8](length=65536, fill=0)

    def slot(mut self, fd: Int):
        while len(self.conns) <= fd:
            self.conns.append(Conn())

    def watch(mut self, fd: Int, want: Bool, add: Bool):
        var mask = UInt32(EPOLLIN | EPOLLRDHUP)
        if want:
            mask = mask | UInt32(EPOLLOUT)
        var ev = EpollEvent(mask, Int32(fd))
        var op = EPOLL_CTL_ADD if add else EPOLL_CTL_MOD
        _ = external_call["epoll_ctl", c_int](self.epfd, c_int(op), c_int(fd), Pointer(to=ev))
        self.conns[fd].want_out = want

    def open_fd(mut self, fd: Int, upstream: Bool):
        self.slot(fd)
        self.conns[fd] = Conn()
        self.conns[fd].active = True
        self.conns[fd].upstream = upstream

    def close_fd(mut self, fd: Int):
        if fd < 0 or fd >= len(self.conns) or not self.conns[fd].active:
            return
        var ev = EpollEvent(0, Int32(fd))
        _ = external_call["epoll_ctl", c_int](self.epfd, c_int(EPOLL_CTL_DEL), c_int(fd), Pointer(to=ev))
        _ = external_call["close", c_int](c_int(fd))
        var peer = self.conns[fd].peer
        self.conns[fd] = Conn()
        if peer >= 0 and peer < len(self.conns) and self.conns[peer].active:
            self.conns[peer].peer = -1
            if self.conns[peer].upstream:
                self.close_fd(peer)  # client went away: drop its upstream
            elif self.conns[peer].state == C_TUNNEL:
                self.conns[peer].close_after = True
                self.flush(peer)
            elif self.conns[peer].state == C_WAITING:
                self.bad_gateway(peer)

    # -- output ---------------------------------------------------------------

    def send_bytes(mut self, fd: Int, data: List[UInt8]):
        self.conns[fd].out.extend(data.copy())
        self.flush(fd)

    def flush(mut self, fd: Int):
        if fd < 0 or fd >= len(self.conns) or not self.conns[fd].active:
            return
        while self.conns[fd].out_off < len(self.conns[fd].out):
            var off = self.conns[fd].out_off
            var n = external_call["send", Int](
                c_int(fd), self.conns[fd].out.unsafe_ptr().unsafe_offset(off),
                c_size_t(len(self.conns[fd].out) - off), c_int(0x4000))  # MSG_NOSIGNAL
            if n < 0:
                if errno() == EAGAIN:
                    break
                self.close_fd(fd)
                return
            self.conns[fd].out_off += n
        if self.conns[fd].out_off >= len(self.conns[fd].out):
            self.conns[fd].out = List[UInt8]()
            self.conns[fd].out_off = 0
            if self.conns[fd].want_out:
                self.watch(fd, False, False)
            if self.conns[fd].close_after and not self.conns[fd].upstream:
                self.close_fd(fd)
        elif not self.conns[fd].want_out:
            self.watch(fd, True, False)

    # -- upstream ---------------------------------------------------------------

    def connect_upstream(mut self, client: Int) -> Int:
        var ufd = Int(external_call["socket", c_int](c_int(AF_INET), c_int(SOCK_STREAM), c_int(0)))
        if ufd < 0:
            return -1
        var on = c_int(1)
        _ = external_call["ioctl", c_int](c_int(ufd), c_int(FIONBIO), Pointer(to=on))
        _ = external_call["setsockopt", c_int](c_int(ufd), c_int(IPPROTO_TCP), c_int(TCP_NODELAY),
                                               Pointer(to=on), c_size_t(4))
        var addr = SockAddrIn(self.upstream_port, self.upstream_host_ip)
        var rc = external_call["connect", c_int](c_int(ufd), Pointer(to=addr), c_int(size_of[SockAddrIn]()))
        if rc < 0 and errno() != EINPROGRESS:
            _ = external_call["close", c_int](c_int(ufd))
            return -1
        self.open_fd(ufd, True)
        self.conns[ufd].state = U_CONNECTING
        self.conns[ufd].peer = client
        self.conns[client].peer = ufd
        self.watch(ufd, True, True)
        return ufd

    def bad_gateway(mut self, client: Int):
        var body = String('{"detail":"Bad Gateway"}')
        var out = List[UInt8]()
        append_str(out, "HTTP/1.1 502 Bad Gateway\r\nserver: mojo-gate\r\ncontent-type: application/json\r\ncontent-length: "
                   + String(body.byte_length()) + "\r\nconnection: close\r\n\r\n" + body)
        self.conns[client].close_after = True
        self.conns[client].state = C_IDLE
        self.send_bytes(client, out)

    def bad_request(mut self, client: Int, msg: String = "Bad Request"):
        var body = String('{"detail":"') + msg + String('"}')
        var out = List[UInt8]()
        append_str(out, "HTTP/1.1 400 Bad Request\r\ndate: " + http_date()
                   + "\r\nserver: mojo-gate\r\ncontent-type: application/json\r\ncontent-length: "
                   + String(body.byte_length()) + "\r\nconnection: close\r\n\r\n" + body)
        self.conns[client].close_after = True
        self.conns[client].state = C_IDLE
        self.send_bytes(client, out)

    def forbidden(mut self, client: Int, msg: String = "Forbidden"):
        var body = String('{"detail":"') + msg + String('"}')
        var out = List[UInt8]()
        append_str(out, "HTTP/1.1 403 Forbidden\r\ndate: " + http_date()
                   + "\r\nserver: mojo-gate\r\ncontent-type: application/json\r\ncontent-length: "
                   + String(body.byte_length()) + "\r\nconnection: close\r\n\r\n" + body)
        self.conns[client].close_after = True
        self.conns[client].state = C_IDLE
        self.send_bytes(client, out)

    def payload_too_large(mut self, client: Int):
        var body = String('{"detail":"Payload Too Large"}')
        var out = List[UInt8]()
        append_str(out, "HTTP/1.1 413 Payload Too Large\r\ndate: " + http_date()
                   + "\r\nserver: mojo-gate\r\ncontent-type: application/json\r\ncontent-length: "
                   + String(body.byte_length()) + "\r\nconnection: close\r\n\r\n" + body)
        self.conns[client].close_after = True
        self.conns[client].state = C_IDLE
        self.send_bytes(client, out)

    def on_upstream_data(mut self, ufd: Int, n: Int):
        var client = self.conns[ufd].peer
        var data = List[UInt8](self.tmp[0:n])
        if client < 0:
            return
        if self.conns[client].state == C_TUNNEL:
            self.send_bytes(client, data)
            return
        if not self.conns[ufd].hdr_done:
            self.conns[ufd].resp.extend(data^)
            var hend = find_crlfcrlf(self.conns[ufd].resp, 0)
            if hend < 0:
                if len(self.conns[ufd].resp) > MAX_HEADER_BYTES:
                    self.close_fd(ufd)
                return
            self.forward_head(ufd, client, hend)
        else:
            self.conns[ufd].body_seen += n
            if self.conns[ufd].capture:
                if len(self.conns[ufd].resp) + n > self.entry_max_bytes:
                    self.conns[ufd].capture = False
                    self.conns[ufd].resp = List[UInt8]()
                else:
                    self.conns[ufd].resp.extend(data.copy())
            self.send_bytes(client, data)

    def forward_head(mut self, ufd: Int, client: Int, hend: Int):
        """Response head arrived: decide framing, rewrite `connection`, forward."""
        var resp = self.conns[ufd].resp.copy()
        var head = latin1(resp, 0, hend)
        var status = 0
        var sp = head.find(" ")
        if sp > 0:
            status = parse_int(String(head[byte=sp + 1:sp + 4]))
        var lines = head.split("\r\n")
        var cl = -1
        var chunked = False
        var set_cookie = False
        var no_store = False
        for i in range(1, len(lines)):
            var l = String(lines[i])
            var colon = l.find(":")
            if colon <= 0:
                continue
            var name = lower_ascii(String(l[byte=0:colon]))
            var value = strip_ws(String(l[byte=colon + 1:l.byte_length()]))
            if name == "content-length":
                cl = parse_int(value)
            elif name == "transfer-encoding":
                chunked = lower_ascii(value).find("chunked") >= 0
            elif name == "set-cookie":
                set_cookie = True
            elif name == "cache-control":
                var lv = lower_ascii(value)
                if (lv.find("no-store") >= 0 or lv.find("private") >= 0
                    or lv.find("no-cache") >= 0 or lv.find("max-age=0") >= 0
                    or lv.find("s-maxage=0") >= 0):
                    no_store = True
            elif name == "vary":
                var lv = lower_ascii(value)
                if lv.find("*") >= 0:
                    no_store = True
            elif name == "content-range":
                no_store = True
        if cl >= 0 and chunked:
            # Dual framing from upstream: reject with 502 Bad Gateway
            self.bad_gateway(client)
            self.close_fd(ufd)
            return
        if status == 101:
            self.conns[client].state = C_TUNNEL
            self.conns[ufd].state = C_TUNNEL
        var no_body = self.conns[ufd].head_req or status == 204 or status == 304 or (status >= 100 and status < 200)
        var delimited = no_body or cl >= 0 or chunked
        var keep = self.conns[client].keep_alive and delimited
        self.conns[client].keep_alive = keep
        self.conns[ufd].cl = 0 if no_body else cl
        self.conns[ufd].chunked = chunked and not no_body
        self.conns[ufd].hdr_done = True
        self.conns[ufd].body_seen = len(resp) - hend
        if self.conns[ufd].cache_key.byte_length() > 0 and status == 200 and not set_cookie and not no_store and not chunked and delimited:
            self.conns[ufd].capture = True
        else:
            self.conns[ufd].capture = False

        # forward head (dropping `connection: close` for keep-alive clients) + body so far
        var out = List[UInt8]()
        var line_start = 0
        var i = 0
        while i + 1 < hend:
            if resp[i] == 13 and resp[i + 1] == 10:
                var drop = False
                if keep and i - line_start == 17:
                    drop = lower_ascii(latin1(resp, line_start, i)) == "connection: close"
                if not drop:
                    for k in range(line_start, i + 2):
                        out.append(resp[k])
                line_start = i + 2
                i += 2
                if line_start == hend - 2:
                    break
                continue
            i += 1
        append_str(out, "\r\n")
        for k in range(hend, len(resp)):
            out.append(resp[k])
        if not self.conns[ufd].capture:
            self.conns[ufd].resp = List[UInt8]()
        self.send_bytes(client, out)

    def on_upstream_eof(mut self, ufd: Int):
        var client = self.conns[ufd].peer
        if client >= 0 and self.conns[client].active:
            if self.conns[client].state == C_TUNNEL:
                self.conns[client].close_after = True
                self.conns[ufd].peer = -1
                self.conns[client].peer = -1
                self.close_fd(ufd)
                self.flush(client)
                return
            if not self.conns[ufd].hdr_done:
                self.conns[ufd].peer = -1
                self.conns[client].peer = -1
                self.close_fd(ufd)
                self.bad_gateway(client)
                return
            if self.conns[ufd].capture:
                self.store(ufd)
            self.conns[ufd].peer = -1
            self.conns[client].peer = -1
            self.close_fd(ufd)
            self.conns[client].state = C_IDLE
            if not self.conns[client].keep_alive:
                self.conns[client].close_after = True
                self.flush(client)
                return
            self.process(client)
        else:
            self.close_fd(ufd)

    def store(mut self, ufd: Int):
        var resp = self.conns[ufd].resp.copy()
        var hend = find_crlfcrlf(resp, 0)
        if hend < 0:
            return
        var body_len = len(resp) - hend
        if self.conns[ufd].chunked:
            return  # never cache chunked responses
        if self.conns[ufd].cl >= 0 and body_len != self.conns[ufd].cl:
            return  # truncated

        # Find status line end
        var sl_end = 0
        while sl_end + 1 < hend and not (resp[sl_end] == 13 and resp[sl_end + 1] == 10):
            sl_end += 1
        var status_line = List[UInt8](resp[0:sl_end + 2])

        # Find date: header anywhere in the response
        var d_start = -1
        var cur = sl_end + 2
        while cur + 1 < hend:
            var le = cur
            while le + 1 < hend and not (resp[le] == 13 and resp[le + 1] == 10):
                le += 1
            if le == cur:
                break
            if le - cur >= 5 and lower_ascii(latin1(resp, cur, cur + 5)) == "date:":
                d_start = cur
                break
            cur = le + 2

        var rest_close = List[UInt8]()
        var rest_keep = List[UInt8]()

        var ls = sl_end + 2
        while ls + 1 < hend:
            var le = ls
            while le + 1 < hend and not (resp[le] == 13 and resp[le + 1] == 10):
                le += 1
            if le == ls:
                break
            if ls != d_start:
                var is_conn_close = (le - ls == 17 and lower_ascii(latin1(resp, ls, le)) == "connection: close")
                if not is_conn_close:
                    for k in range(ls, le + 2):
                        rest_keep.append(resp[k])
                for k in range(ls, le + 2):
                    rest_close.append(resp[k])
            ls = le + 2

        append_str(rest_close, "\r\n")
        append_str(rest_keep, "\r\n")

        var body = List[UInt8](resp[hend:len(resp)])
        var e = Entry(status_line^, rest_close^, rest_keep^, body^, now_s())
        if e.size > self.entry_max_bytes:
            return
        if self.cache_bytes + e.size > self.cache_max_bytes:
            self.sweep()
            if self.cache_bytes + e.size > self.cache_max_bytes:
                return
        var key = self.conns[ufd].cache_key
        if key in self.cache:
            try:
                self.cache_bytes -= self.cache[key].size
            except:
                pass
        self.cache_bytes += e.size
        self.cache[key] = e^

    def sweep(mut self):
        var now = now_s()
        var dead = List[String]()
        for item in self.cache.items():
            if now - item.value.born >= self.cache_ttl_s:
                dead.append(item.key)
        for k in dead:
            try:
                var e = self.cache.pop(k)
                self.cache_bytes -= e.size
            except:
                pass

    def flush_cache(mut self):
        self.cache = Dict[String, Entry]()
        self.cache_bytes = 0

    def is_cacheable(self, r: Request) -> Bool:
        if not (r.method == "GET" or r.method == "HEAD"):
            return False
        if r.version != "HTTP/1.1" or r.uncacheable_hdr or r.non_ascii or r.body_len > 0 or r.chunked:
            return False
        if r.target.byte_length() > MAX_TARGET_BYTES:
            return False
        var p = r.raw_path
        if self.purge_endpoint.byte_length() > 0 and p == self.purge_endpoint:
            return False
        for i in range(len(self.no_cache_prefixes)):
            var prefix = self.no_cache_prefixes[i]
            if prefix.byte_length() > 0 and p.startswith(prefix):
                return False
        return True

    # -- rate limiting -------------------------------------------------------------

    def rate_check(mut self, path: List[UInt8], ip: String) -> Int:
        """Return 0 if allowed, else retry_after * 100000 + limit."""
        if not self.rate_limit or len(self.rate_rules) == 0:
            return 0
        var matched = -1
        for i in range(len(self.rate_rules)):
            if bytes_startswith(path, self.rate_rules[i].prefix):
                matched = i
                break
        if matched < 0:
            return 0

        var rule = self.rate_rules[matched]
        var prefix = rule.prefix
        var limit = rule.limit
        var window = rule.window_s

        var key = ip + "|" + prefix
        var now_ms = Int(monotonic() // 1_000_000)
        if key not in self.buckets:
            if len(self.buckets) >= MAX_BUCKETS:
                self.prune_buckets()
                if len(self.buckets) >= MAX_BUCKETS:
                    return 0  # fail-open on bucket table overflow to avoid OOM
            self.buckets[key] = List[Int]()
        try:
            var b = self.buckets[key].copy()
            var start = 0
            while start < len(b) and now_ms - b[start] > window * 1000:
                start += 1
            var kept = List[Int](b[start:len(b)])
            if len(kept) >= limit:
                var elapsed_ms = now_ms - kept[0]
                var retry = (window * 1000 - elapsed_ms) // 1000 + 1
                if retry < 1:
                    retry = 1
                self.buckets[key] = kept^
                return retry * 100000 + limit
            kept.append(now_ms)
            self.buckets[key] = kept^
        except:
            pass
        return 0

    def too_many(mut self, client: Int, retry: Int, limit: Int):
        var msg = self.rate_limit_msg
        var body: String
        if msg.find("{retry}") >= 0:
            var parts = msg.split("{retry}")
            body = String(parts[0]) + String(retry) + (String(parts[1]) if len(parts) > 1 else "")
        else:
            body = msg
        var out = List[UInt8]()
        append_str(out, "HTTP/1.1 429 Too Many Requests\r\ndate: " + http_date()
                   + "\r\nserver: mojo-gate\r\nretry-after: " + String(retry)
                   + "\r\nx-ratelimit-limit: " + String(limit)
                   + "\r\nx-ratelimit-remaining: 0\r\ncontent-length: " + String(body.byte_length())
                   + "\r\ncontent-type: application/json\r\n")
        if not self.conns[client].keep_alive:
            append_str(out, "connection: close\r\n")
        append_str(out, "\r\n" + body)
        self.send_bytes(client, out)

    # -- purge handling -----------------------------------------------------------

    def handle_purge(mut self, client: Int, keep: Bool):
        self.flush_cache()
        var body = String('{"status":"ok","purged":true}')
        var out = List[UInt8]()
        append_str(out, "HTTP/1.1 200 OK\r\ndate: " + http_date()
                   + "\r\nserver: mojo-gate\r\ncontent-type: application/json\r\ncontent-length: "
                   + String(body.byte_length()) + "\r\n")
        if not keep:
            append_str(out, "connection: close\r\n")
        append_str(out, "\r\n" + body)
        self.send_bytes(client, out)

    # -- request processing --------------------------------------------------------

    def process(mut self, client: Int):
        """Handle every complete request queued on `client` while it is idle."""
        while self.conns[client].active and self.conns[client].state == C_IDLE \
                and not self.conns[client].close_after:
            var inbuf = self.conns[client].inbuf.copy()
            var hend = find_crlfcrlf(inbuf, 0)
            if hend < 0:
                if len(inbuf) > MAX_HEADER_BYTES:
                    self.bad_request(client, "Header size exceeds limit")
                return
            var r = parse_request(inbuf, self.internal_token)
            if not r.ok:
                if r.bad_status == 413:
                    self.payload_too_large(client)
                elif r.bad_status == 501:
                    var body = String('{"detail":"Not Implemented"}')
                    var out = List[UInt8]()
                    append_str(out, "HTTP/1.1 501 Not Implemented\r\ncontent-length: "
                               + String(body.byte_length()) + "\r\nconnection: close\r\n\r\n" + body)
                    self.conns[client].close_after = True
                    self.send_bytes(client, out)
                else:
                    self.bad_request(client, "Malformed request or invalid headers")
                return
            if r.chunked or r.upgrade or r.expect or r.method == "CONNECT":
                self.start_tunnel(client, r, inbuf)
                return
            if len(inbuf) < r.head_len + r.body_len:
                return  # wait for the body
            var consumed = r.head_len + r.body_len
            self.conns[client].inbuf = List[UInt8](inbuf[consumed:len(inbuf)])
            var keep = r.version == "HTTP/1.1" and not r.conn_close
            self.conns[client].keep_alive = keep

            # Block external access to internal analytics endpoint
            if self.analytics_endpoint.byte_length() > 0 and r.raw_path == self.analytics_endpoint:
                self.forbidden(client, "Direct access to internal analytics is forbidden")
                return

            # Check cache purge endpoint (allow only from loopback or internal token)
            if self.purge_endpoint.byte_length() > 0 and r.raw_path == self.purge_endpoint and (r.method == "POST" or r.method == "DELETE" or r.method == "PURGE"):
                var peer_ip = self.conns[client].client_ip
                if peer_ip != "127.0.0.1" and peer_ip != "::1" and not r.token_matched:
                    self.forbidden(client, "Cache purge allowed only from loopback")
                    return
                self.handle_purge(client, keep)
                if not keep:
                    self.conns[client].close_after = True
                    self.flush(client)
                return

            var path = percent_decode_bytes(r.raw_path)
            # Determine client IP: only trust X-Forwarded-For if request came from trusted loopback
            var ip = self.conns[client].client_ip
            if r.has_xff and (ip == "127.0.0.1" or ip == "::1"):
                ip = client_ip(r)

            var verdict = self.rate_check(path, ip)
            if verdict > 0:
                self.too_many(client, verdict // 100000, verdict % 100000)
                if not keep:
                    self.conns[client].close_after = True
                    self.flush(client)
                    return
                continue

            var key = String("")
            if self.is_cacheable(r):
                key = cache_key(r, ip == "127.0.0.1" or ip == "::1")
                if self.replay(client, key, keep):
                    if self.analytics_endpoint.byte_length() > 0 and len(self.hits) < MAX_HITS:
                        self.hits.append("[" + json_str(r.raw_path) + "," + json_str(r.raw_query) + ","
                                         + json_str(ip) + "," + json_str(r.user_agent) + "]")
                    if not keep:
                        self.conns[client].close_after = True
                        self.flush(client)
                        return
                    continue

            if not (r.method == "GET" or r.method == "HEAD"):
                self.flush_cache()  # Any write request flushes cache
            self.proxy(client, r, inbuf, key)
            return

    def replay(mut self, client: Int, key: String, keep: Bool) -> Bool:
        """Send the cached response for `key` if present and fresh."""
        var out = List[UInt8]()
        try:
            if key not in self.cache or now_s() - self.cache[key].born >= self.cache_ttl_s:
                return False
            out.extend(self.cache[key].status_line.copy())
            append_str(out, "date: " + http_date() + "\r\n")
            if keep:
                out.extend(self.cache[key].rest_keep.copy())
            else:
                out.extend(self.cache[key].rest_close.copy())
            out.extend(self.cache[key].body.copy())
        except:
            return False
        self.send_bytes(client, out)
        return True

    def proxy(mut self, client: Int, r: Request, inbuf: List[UInt8], key: String):
        var ufd = self.connect_upstream(client)
        if ufd < 0:
            self.bad_gateway(client)
            return
        self.conns[client].state = C_WAITING
        self.conns[ufd].cache_key = key
        self.conns[ufd].head_req = r.method == "HEAD"
        self.conns[ufd].out = upstream_request(r, inbuf, self.conns[client].client_ip)

    def start_tunnel(mut self, client: Int, r: Request, inbuf: List[UInt8]):
        var ufd = self.connect_upstream(client)
        if ufd < 0:
            self.bad_gateway(client)
            return
        self.conns[ufd].out = upstream_request(r, inbuf, self.conns[client].client_ip)
        if not r.upgrade:
            self.conns[client].state = C_TUNNEL
            for k in range(r.head_len, len(inbuf)):
                self.conns[ufd].out.append(inbuf[k])
            self.conns[client].inbuf = List[UInt8]()
        else:
            self.conns[client].state = C_WAITING
            if len(inbuf) > r.head_len:
                self.conns[client].inbuf = List[UInt8](inbuf[r.head_len:len(inbuf)])
            else:
                self.conns[client].inbuf = List[UInt8]()

    # -- event handlers ------------------------------------------------------------

    def on_client_data(mut self, fd: Int, n: Int):
        var data = List[UInt8](self.tmp[0:n])
        if self.conns[fd].state == C_TUNNEL:
            var peer = self.conns[fd].peer
            if peer >= 0:
                self.send_bytes(peer, data)
            return
        if len(self.conns[fd].inbuf) + n > MAX_INBUF_BYTES:
            self.payload_too_large(fd)
            self.close_fd(fd)
            return
        self.conns[fd].inbuf.extend(data^)
        if self.conns[fd].state == C_IDLE:
            self.process(fd)

    def on_event(mut self, fd: Int, events: UInt32):
        if fd >= len(self.conns) or not self.conns[fd].active:
            return
        self.conns[fd].last_active = now_s()
        if self.conns[fd].upstream and self.conns[fd].state == U_CONNECTING:
            if (events & UInt32(EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0:
                var err = c_int(0)
                var elen = c_int(4)
                _ = external_call["getsockopt", c_int](c_int(fd), c_int(SOL_SOCKET), c_int(SO_ERROR),
                                                       Pointer(to=err), Pointer(to=elen))
                if err != 0:
                    var client = self.conns[fd].peer
                    self.conns[fd].peer = -1
                    self.close_fd(fd)
                    if client >= 0 and self.conns[client].active:
                        self.conns[client].peer = -1
                        self.bad_gateway(client)
                    return
                self.conns[fd].state = U_OPEN
                self.flush(fd)
                return
        if (events & UInt32(EPOLLOUT)) != 0:
            self.flush(fd)
            if fd >= len(self.conns) or not self.conns[fd].active:
                return
        if (events & UInt32(EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0:
            while True:
                var n = external_call["recv", Int](c_int(fd), self.tmp.unsafe_ptr(), c_size_t(65536), c_int(0))
                if n > 0:
                    if self.conns[fd].upstream:
                        self.on_upstream_data(fd, n)
                    else:
                        self.on_client_data(fd, n)
                    if fd >= len(self.conns) or not self.conns[fd].active:
                        return
                    if n < 65536:
                        break
                    continue
                if n < 0 and errno() == EAGAIN:
                    break
                # EOF or error
                if self.conns[fd].upstream:
                    self.on_upstream_eof(fd)
                else:
                    self.close_fd(fd)
                return

    # -- side channels -------------------------------------------------------------

    def report_hits(mut self):
        if len(self.hits) == 0 or self.analytics_endpoint.byte_length() == 0:
            return
        var body = String("[")
        for i in range(len(self.hits)):
            if i > 0:
                body += ","
            body += self.hits[i]
        body += "]"
        self.hits = List[String]()
        var fd = external_call["socket", c_int](c_int(AF_INET), c_int(SOCK_STREAM), c_int(0))
        if fd < 0:
            return
        var tv = TimeVal(2)
        _ = external_call["setsockopt", c_int](fd, c_int(SOL_SOCKET), c_int(SO_RCVTIMEO), Pointer(to=tv), c_size_t(16))
        _ = external_call["setsockopt", c_int](fd, c_int(SOL_SOCKET), c_int(SO_SNDTIMEO), Pointer(to=tv), c_size_t(16))
        var addr = SockAddrIn(self.upstream_port, self.upstream_host_ip)
        if external_call["connect", c_int](fd, Pointer(to=addr), c_int(size_of[SockAddrIn]())) == 0:
            var req = List[UInt8]()
            append_str(req, "POST " + self.analytics_endpoint + " HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                       + "Content-Type: application/json\r\nConnection: close\r\n"
                       + (("X-Mojo-Gate-Token: " + self.internal_token + "\r\n") if self.internal_token.byte_length() > 0 else "")
                       + "Content-Length: "
                       + String(body.byte_length()) + "\r\n\r\n" + body)
            var off = 0
            while off < len(req):
                var n = external_call["send", Int](fd, req.unsafe_ptr().unsafe_offset(off),
                                                   c_size_t(len(req) - off), c_int(0x4000))
                if n <= 0:
                    break
                off += n
            while external_call["recv", Int](fd, self.tmp.unsafe_ptr(), c_size_t(65536), c_int(0)) > 0:
                pass
        _ = external_call["close", c_int](fd)

    def prune_buckets(mut self):
        var now_ms = Int(monotonic() // 1_000_000)
        var dead = List[String]()
        for item in self.buckets.items():
            var b = item.value.copy()
            if len(b) == 0 or now_ms - b[len(b) - 1] > 60_000:
                dead.append(item.key)
        for k in dead:
            try:
                _ = self.buckets.pop(k)
            except:
                pass

    def reap_stale(mut self, now: Int):
        for fd in range(len(self.conns)):
            if self.conns[fd].active and not self.conns[fd].upstream:
                if now - self.conns[fd].last_active > 30:
                    self.close_fd(fd)


# ─── command-line and main ───────────────────────────────────────────────────


def parse_rate_rules(s: String) -> List[RateRule]:
    var rules = List[RateRule]()
    if s.byte_length() == 0:
        return rules^
    var parts = s.split(",")
    for i in range(len(parts)):
        var item = strip_ws(String(parts[i]))
        if item.byte_length() == 0:
            continue
        var sub = item.split(":")
        if len(sub) >= 3:
            var pfx = strip_ws(String(sub[0]))
            var lim = parse_int(strip_ws(String(sub[1])))
            var win = parse_int(strip_ws(String(sub[2])))
            if lim > 0 and win > 0 and pfx.byte_length() > 0:
                rules.append(RateRule(pfx, lim, win))
        elif len(sub) == 2:
            var pfx = strip_ws(String(sub[0]))
            var lim = parse_int(strip_ws(String(sub[1])))
            if lim > 0 and pfx.byte_length() > 0:
                rules.append(RateRule(pfx, lim, 60))
    return rules^


def parse_prefixes(s: String) -> List[String]:
    var out = List[String]()
    if s.byte_length() == 0:
        return out^
    var parts = s.split(",")
    for i in range(len(parts)):
        var item = strip_ws(String(parts[i]))
        if item.byte_length() > 0:
            out.append(item)
    return out^


def print_usage():
    print("usage: gate_mojo [options]")
    print("")
    print("Mojo Gate: High-performance reverse proxy & caching gateway module")
    print("")
    print("Options:")
    print("  --host <ip>                Host IP to bind (default: 127.0.0.1)")
    print("  --port <port>              Port to listen on (default: 8080)")
    print("  --upstream-host <ip>       Upstream backend host IP (default: 127.0.0.1)")
    print("  --upstream-port <port>     Upstream backend port (default: 8083)")
    print("  --cache-ttl <seconds>      Cache TTL in seconds (default: 60)")
    print("  --cache-max-bytes <bytes>  Maximum cache size in bytes (default: 268435456)")
    print("  --entry-max-bytes <bytes>  Maximum entry size in bytes (default: 8388608)")
    print("  --no-rate-limit            Disable rate limiting")
    print("  --rate-rules <rules>       Rate limit rules prefix:limit[:window_s], comma-separated")
    print("  --rate-limit-msg <text>    JSON response on 429 ({retry} placeholder supported)")
    print("  --no-cache-prefixes <csv>  Comma-separated path prefixes to exclude from cache")
    print("  --analytics-endpoint <url> Upstream endpoint for hit reporting (empty to disable)")
    print("  --purge-endpoint <url>     Endpoint to purge cache via POST/DELETE (default: /_mojo_gate/purge)")
    print("  -h, --help                 Show this help message and exit")


def main() raises:
    var host_str = String("127.0.0.1")
    var port = 8080
    var upstream_host_str = String("127.0.0.1")
    var upstream_port = 8083
    var cache_ttl = 60
    var cache_max_bytes = 256 * 1024 * 1024
    var entry_max_bytes = 8 * 1024 * 1024
    var rate_limit = True
    var rate_rules_str = String("")
    var rate_limit_msg = String('{"detail":"Too Many Requests","retry_after":{retry}}')
    var no_cache_prefixes_str = String("/_mojo_gate")
    var analytics_endpoint = String("")
    var purge_endpoint = String("/_mojo_gate/purge")
    var internal_token = String("")

    var args = argv()
    var i = 1
    while i < len(args):
        var a = String(args[i])
        if (a == "-h" or a == "--help"):
            print_usage()
            return
        elif a == "--host" and i + 1 < len(args):
            host_str = String(args[i + 1])
            i += 2
        elif a == "--port" and i + 1 < len(args):
            port = parse_int(String(args[i + 1]))
            i += 2
        elif a == "--upstream-host" and i + 1 < len(args):
            upstream_host_str = String(args[i + 1])
            i += 2
        elif a == "--upstream-port" and i + 1 < len(args):
            upstream_port = parse_int(String(args[i + 1]))
            i += 2
        elif a == "--cache-ttl" and i + 1 < len(args):
            cache_ttl = parse_int(String(args[i + 1]))
            i += 2
        elif a == "--cache-max-bytes" and i + 1 < len(args):
            cache_max_bytes = parse_int(String(args[i + 1]))
            i += 2
        elif a == "--entry-max-bytes" and i + 1 < len(args):
            entry_max_bytes = parse_int(String(args[i + 1]))
            i += 2
        elif a == "--no-rate-limit":
            rate_limit = False
            i += 1
        elif a == "--rate-rules" and i + 1 < len(args):
            rate_rules_str = String(args[i + 1])
            i += 2
        elif a == "--rate-limit-msg" and i + 1 < len(args):
            rate_limit_msg = String(args[i + 1])
            i += 2
        elif a == "--no-cache-prefixes" and i + 1 < len(args):
            no_cache_prefixes_str = String(args[i + 1])
            i += 2
        elif a == "--analytics-endpoint" and i + 1 < len(args):
            analytics_endpoint = String(args[i + 1])
            i += 2
        elif a == "--internal-token" and i + 1 < len(args):
            internal_token = String(args[i + 1])
            i += 2
        else:
            i += 1

    var host_ip = parse_ipv4(host_str)
    var upstream_host_ip = parse_ipv4(upstream_host_str)
    var rate_rules = parse_rate_rules(rate_rules_str)
    var no_cache_prefixes = parse_prefixes(no_cache_prefixes_str)

    var p = Proxy(upstream_host_ip, upstream_port, cache_ttl, cache_max_bytes, entry_max_bytes,
                  rate_limit, rate_rules^, rate_limit_msg, no_cache_prefixes^,
                  analytics_endpoint, purge_endpoint, internal_token)

    var lfd = external_call["socket", c_int](c_int(AF_INET), c_int(SOCK_STREAM), c_int(0))
    if lfd < 0:
        print("[mojo-gate] error: cannot create socket")
        return
    var on = c_int(1)
    _ = external_call["setsockopt", c_int](lfd, c_int(SOL_SOCKET), c_int(SO_REUSEADDR), Pointer(to=on), c_size_t(4))
    var addr = SockAddrIn(port, host_ip)
    if external_call["bind", c_int](lfd, Pointer(to=addr), c_int(size_of[SockAddrIn]())) < 0:
        print("[mojo-gate] error: cannot bind " + host_str + ":" + String(port))
        _ = external_call["close", c_int](lfd)
        return
    _ = external_call["listen", c_int](lfd, c_int(1024))
    _ = external_call["ioctl", c_int](lfd, c_int(FIONBIO), Pointer(to=on))
    p.listen_fd = lfd
    p.epfd = external_call["epoll_create1", c_int](c_int(0))
    var lev = EpollEvent(UInt32(EPOLLIN), lfd)
    _ = external_call["epoll_ctl", c_int](p.epfd, c_int(EPOLL_CTL_ADD), lfd, Pointer(to=lev))

    print("[mojo-gate] proxy listening on " + host_str + ":" + String(port)
          + " -> upstream " + upstream_host_str + ":" + String(upstream_port))

    var caddr = SockAddrIn(0)
    var events = List[EpollEvent]()
    for _ in range(256):
        events.append(EpollEvent(0, 0))
    var last_tick = now_s() - 10
    var last_sweep = now_s()

    while True:
        var nfds = external_call["epoll_wait", c_int](p.epfd, events.unsafe_ptr(), c_int(256), c_int(500))
        for k in range(Int(nfds)):
            var fd = Int(events[k].fd)
            if fd == Int(lfd):
                while True:
                    var caddr_len = c_int(size_of[SockAddrIn]())
                    var cfd = Int(external_call["accept", c_int](lfd, Pointer(to=caddr), Pointer(to=caddr_len)))
                    if cfd < 0:
                        break
                    _ = external_call["ioctl", c_int](c_int(cfd), c_int(FIONBIO), Pointer(to=on))
                    _ = external_call["setsockopt", c_int](c_int(cfd), c_int(IPPROTO_TCP), c_int(TCP_NODELAY),
                                                           Pointer(to=on), c_size_t(4))
                    p.open_fd(cfd, False)
                    p.conns[cfd].client_ip = format_ipv4(caddr.sin_addr)
                    p.watch(cfd, False, True)
            else:
                p.on_event(fd, events[k].events)

        var now = now_s()
        if now - last_tick >= 1:
            last_tick = now
            p.report_hits()

        if now - last_sweep >= 5:
            last_sweep = now
            p.sweep()
            p.prune_buckets()
            p.reap_stale(now)

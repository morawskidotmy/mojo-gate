# Performance notes

This file records optimization decisions and the risks knowingly accepted for
them, so future maintenance does not "clean up" an intentional coupling or
assume an unverified property.

## Landed optimizations

- **Bulk byte copies.** `append_str` uses `List.extend(String.as_bytes())`;
  hot loops (`send_bytes`, `on_client_data`, `on_upstream_data`,
  `forward_head`, `start_tunnel`, `upstream_request`, `report_hits`) use
  slice + `extend` instead of per-byte `append`.
- **Cache-hit replay.** `replay` builds into the persistent `self.replay_buf`
  (cleared, capacity retained) and emits the prebuilt `self.cached_date_line`
  via `send_replay`; no per-hit output allocation or date concatenation.
- **Single head scan.** `parse_request` accepts the CRLFCRLF offset already
  found by `process`.
- **One fewer syscall.** `connect_upstream` uses
  `socket(AF_INET, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC, 0)` (no `ioctl`).
- **Path fast paths.** `percent_decode_bytes` is skipped when the raw path has
  no `%`; `normalize_path` returns early for already-canonical paths.
- **Replay TTL.** `replay` reuses `current_date()`'s cached second instead of a
  second `monotonic()` call.
- **Analytics body.** `report_hits` builds its JSON in a byte buffer instead of
  O(n^2) `String +=`.

## Consciously accepted risks / invariants

1. **Header re-encoding relies on parser-enforced ASCII headers.**
   `upstream_request` re-encodes header lines with `as_bytes()`, which is only
   byte-preserving because `parse_request` rejects any header byte `> 126`.
   Do not relax that validation without revisiting the re-encode.
2. **`replay_buf` retains peak response capacity** (up to `entry_max_bytes`,
   8 MB) for the process lifetime after a large cache hit. Bounded; acceptable.
3. **`send_replay` duplicates `send_bytes`' partial-send / `close_after` logic.**
   Any fix to one must be applied to the other (or they should be unified via a
   raw-pointer `send_raw`).
4. **The shared `replay_buf` is only safe because the loop is single-threaded.**
   Adding threading would require per-connection scratch buffers.
5. **`List.clear()` capacity retention is assumed, not verified** against the
   Mojo 1.1.0 stdlib (shipped precompiled). If `clear()` freed the buffer, the
   replay optimization would be neutral but still correct.
6. **Latent O(n^2) copy under HTTP pipelining.** `process` copies the whole
   `inbuf` per request. Removing it (move-out) is not expressible in this Mojo
   version (`error: expression does not designate a value with an origin` when
   moving out of an indexed list element). A read-offset cursor would be needed.
7. **`report_hits` blocks the event loop** once per second on a loopback
   connect/send/recv (2 s socket timeouts). Analytics is an optional feature;
   accepted.

## Measurement

`tests/bench_smoke.py` reports proxy CPU time per request from
`/proc/<pid>/stat` and pins the proxy to `BENCH_CPU` (default core 0). A clean
A/B on an idle machine (best of 3, 60k cache-hit keep-alive requests, 4 Python
threads) measured **7.00 us/req -> 5.33 us/req (~24% less proxy CPU)** for the
first performance round. Absolute numbers are generator-bound (the Python
loader is GIL-limited); compare builds, not absolute throughput, and run on an
idle host.

# BOBS profile after CRC32C removal (steady write load, conc 128, 8 BOBS, ~1 GiB/s)

Captured: /debug/pprof/profile?seconds=30 on bobs-1 under sustained loadgen (loadgen Running before+after; BOBS ~7c). File: nocrc_flame_write_c128.svg

## Cumulative CPU (% of on-CPU samples)
- HTTP/2 framing (hyper::proto::h2 + h2 codec):  ~37-40%   <-- NEW BOTTLENECK
    - hyper h2 Server::poll 39.4, h2 Connection::poll 36.4, FramedRead::poll_next 19.3, FramedWrite 9.1, recv_data 7.2
- spool write/create/complete logic:             ~10-15%  (write_spool 11.4, create_spool 6.8, complete 4.6)
- actual disk write (io_uring write_at + sync):  ~5-7%    (writer::write 3.4, write_at 0.8, sync_data 0.4, memcpy 0.4)
- page_cache / mutex / parking_lot / locks:      ~1-2%    <-- contention hypothesis DISPROVEN
- redb commit (was 50% in DRAIN capture):        ~0% under writes (cleanup-only artifact)

## Conclusion
After CRC removal, BOBS is CPU-bound in the HTTP/2 codec, not locks (~1%) or disk (~5%).
Cause: 16 MiB bodies over h2 with default 16 KiB MAX_FRAME_SIZE => ~1024 DATA frames/body,
each paying flow-control + stream bookkeeping. Both worker->BOBS (write) and BOBS->reader (read).

## Levers (cheapest first)
1. Bump h2 MAX_FRAME_SIZE toward 16 MiB (2^24-1) + large flow-control windows, both ends. Config-only.
2. Switch worker->BOBS to HTTP/1.1 (content-length stream, no per-frame accounting). Removes h2 entirely on the internal hop.
3. Same for BOBS->reader read path.

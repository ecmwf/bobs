# Core Principle

BOBS (Big-Object Buffered Storage) is a service for buffering of large responses from producers (typically few, producing fast) and consumption by users (typically many, consuming slowly), in a 1-to-1 fashion. It is designed to be used as a temporary store for streaming responses to users. It is not designed for long-term storage, and does not have features like replication or durability. It is essentially a single-reader, single-writer temporary spool per stream, with a simple API for writing and reading data. It also has clean-up mechanisms which try to guess when a stream has been fully read and can be deleted.

It is deployed as a set of pods with a small DNS service which allows direct routing from an external ingress to the correct instance of BOBS. Producers choose a BOBS via a kubernetes service with preferLocal routing (optional).

Each spool is a single contiguous byte stream, paged into page_size (default 4KB) chunks:
Write path:
  HTTP body chunks → bytearray write buffer → when full (4KB):
    1. Snapshot to read_pages[idx] (in-memory cache)
    2. Flush to disk file via async_files
    3. Increment write_idx, fresh buffer
Read path (per page):
  read_pages cache hit? → return it
  Current write buffer? → snapshot + return
  Neither? → disk fetch (opens/reuses async file descriptor)

The "persistent streaming" trick: when the reader is faster than the writer, it long-polls until the next page is available.

We should also consider parallel chunk writing and reading.

HTTP API (5 endpoints)
Method	Route	What it does
GET/HEAD	/status	Health check
PUT	/create	Allocates a new dataset, returns {key: "host-uuid4"}
POST	/write/{key}/{offset}	Streams request body into dataset. Offset must match current head exactly (no gaps, no overwrites)
POST	/close/{key}	Flushes remaining buffer, closes write FD. Makes dataset read-only
GET	/read/{key}	Streams back bytes as application/octet-stream via HTTP Range. No Range (or bytes=X-) = follow mode


When a full dataset has been read, and the client has closed the connection and not returned within a configurable time, the spool is deleted.

If the client has not read the connection and not returned within a configurable time, the spool is deleted.

The original PUT should provide content metadata (content-type).

BOBS should be able to redirect to other BOBS when its storage is nearly full. This might require some coordination via a database. The database might also be the backing for the DNS. This would happen at /create time.

The producer should also be able to tell BOBS to not allow reading until the full stream has been written. There should also be an explicit delete API for the producer to call in case it needs to clean up a spool early.

Writer inactivity should also be handled. Partially written spools should be deleted.

We cannot recover lost data from the producer, we could consider if its possible.

The key -> spool mapping should be persistent (on disk) for recovery.

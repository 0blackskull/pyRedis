# pyRedis: A Toy Redis Implementation in Pure Python

## Project Overview

A minimal, test-driven clone of the Redis key-value store, focused on core data structures and command processing. Designed to demonstrate systems-level Python, network fundamentals, parsing, and event-driven server design.

## How to Run This Project

**Requirements:**

- Python 3.8 or higher
- `pytest` for tests

**To start the server:**

```sh
python app/main.py
```

The server listens on `localhost:6379` by default.

**To run the tests:**

```sh
pytest tests/test_server.py
```

---

## Project Structure

- `app/main.py`: All main server logic, protocol handling, and data storage.
- `tests/test_server.py`: Automated tests executing commands against a live server process via sockets.
- `Pipfile`/`Pipfile.lock`: Project dependencies (minimal).

---

## Architectural Overview

This project is single-process, event-driven, and fully self-contained. The core elements are:

### Evented TCP Server

- Uses Python’s `selectors` module for efficient IO multiplexing, managing multiple client connections in a single process and thread.

### RESP Protocol Parsing

- Implements the Redis Serialization Protocol (RESP), handling fragmented/batched requests as would typically occur over real-world TCP streams.

### Data Storage Layer

- Centralized in-memory storage (`DB` class) tracks all keys, values, and expiry information using static class structures.
- Supports string and list types, using a QuickList (linked node arrays) structure for list operations. Expiry is managed via both “active” sweeps and “lazy” checks on read.

### Command Pipeline

- Commands are parsed, normalized, type-checked, and dispatched according to the RESP-defined protocol. The server enforces argument and type rules and returns protocol-compliant responses and errors.

### Testing

- All correctness is driven by socket-level black box tests which interact directly with the running server process using genuine RESP messages.

## Current Functionality and Boundaries

This server supports basic Redis-like commands for strings and lists (`SET`, `GET`, `RPUSH`, `LPUSH`, `LLEN`, `LRANGE`, `LPOP`, `DEL`, etc.) with proper type and argument validation. Data is fully in-memory, and the implementation does not include persistence, snapshotting, clustering, or authentication. The architecture is single-threaded, using event-based IO concurrency to ensure responsiveness and simplicity. The overall approach closely mirrors core Redis event loop and protocol strategies within a single-file Python context.

## Key Design Decisions

**Concurrency with Selectors over Threads**

This server uses cooperative IO multiplexing (via `selectors`) instead of threads. This decision removes complexity with locks and race conditions, allows for deterministic state management, and provides predictable performance even as connection count increases. It reflects how production systems like Redis favor non-blocking, event-driven interfaces for efficiency and maintainability.

**Selectors Module**

Python’s `selectors` wraps system-level event notification (`select`, `epoll`, or `kqueue`), letting a single thread efficiently manage sockets at scale. This abstraction provides performance and code clarity, especially for an educational or prototyping context.

**QuickList for List Storage**

Lists are implemented using a QuickList data structure—a doubly-linked list where each node holds a small array of values. This strikes a pragmatic compromise between optimal head/tail insertion and memory management, and is inspired by Redis’s internal storage approach. Unlike a flat array, QuickList is efficient for dynamic workloads with frequent prepends and appends.

**Engineering Notes**

- The RESP parser robustly handles line breaks and message boundaries across arbitrary TCP splits, a common source of bugs in naive socket programs.
- TTL expiry logic includes both poll-based (“active”) and access-based (“lazy”) removal, enabling efficient cleanup regardless of system usage patterns.
- The DB logic is static and single-process only, by design, to maintain clarity and focus on core concepts.

## Future Scope

- **Transactions and Pub/Sub:** These features can be integrated by extending the command execution logic and internal state management. The server's command routing and event loop are structured to enable further extension.
- **Persistence:** Adding snapshotting or append-only logging would bring data durability between runs.
- **New Types:** Adding support for sets, sorted sets, and hashes would be a direct extension to the current type system.
- **Clustering/Replication:** These advanced capabilities could be built by extending communication protocols and the current modular architecture.

---

## About the Author

_This project was developed as a portfolio experiment and proof of technical depth in backend/server systems. Reach out via LinkedIn or email if you’d like to discuss it or see further work._

# Changelog

All notable changes to `wren` (OCaml) are documented here.

## [Unreleased]

### Added

- Initial OCaml port of `wren`, the ergonomic RabbitMQ / AMQP library, aiming
  for capability-for-capability parity with the Gleam original and the Dart
  sibling. Built as a friendly, typed layer over
  [`amqp-client-lwt`](https://opam.ocaml.org/packages/amqp-client-lwt/) — a
  pure-OCaml AMQP implementation, so no FFI or C stubs are involved.

  Where Gleam leans on the BEAM and OTP supervision, and Dart on
  `async`/`Future`/`Stream`, this port uses idiomatic `Lwt` promises and an
  explicit backoff-driven recovery loop.

  - **Pure core** — config (incl. `Config.from_env`), codecs (JSON / string /
    bytes), messages and the `kind` convention, topology option types, the full
    publish-options builder surface, retry policy + backoff maths, retry
    metadata headers, the dead-letter / delay-queue topology layout, and the
    kind-routing table. All broker-independent and unit-tested.
  - **IO layer** (over `amqp-client-lwt`) — connection & channel lifecycle,
    QoS/prefetch, queue/exchange/binding declaration with typed `x-*` arguments,
    publishing (raw, text, options, encoded, batch, multi-target, kind-routed),
    publisher confirms, one-off `get`, a router-style consumer dispatching by
    `kind`, retry & dead-letter settlement, a self-healing recoverable consumer,
    a round-robin connection pool, and a `Client` front door.

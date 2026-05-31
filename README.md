# wren

An ergonomic **RabbitMQ / AMQP** messaging library for [OCaml](https://ocaml.org),
built as a friendly, type-safe layer over
[`amqp-client-lwt`](https://opam.ocaml.org/packages/amqp-client-lwt/).

> Small, quick, and busy — a wren is a tiny bird that does a great deal. This
> library aims to do the same for your messages: typed publishing, a
> router-style consumer, retries and dead-lettering, and self-healing
> reconnection, without the boilerplate.

This is the OCaml sibling of the [Gleam `wren`](https://github.com/cargopete/wren)
and [Dart `wren`](https://github.com/cargopete/wren-dart) libraries, ported
capability-for-capability. Where Gleam leans on the BEAM and OTP supervision,
and Dart on `async`/`Future`/`Stream`, the OCaml port uses idiomatic
[`Lwt`](https://ocsigen.org/lwt/) promises and an explicit backoff-driven
recovery loop. Unlike the other two ports (which bind to Erlang's `amqp_client`
and `dart_amqp` respectively), this one wraps a **pure-OCaml** AMQP
implementation — no FFI, no C stubs.

## Status

**In development.** The pure, broker-independent core is complete and
unit-tested; the IO layer wraps `amqp-client-lwt`.

- ✅ Typed connections & channels over `amqp-client-lwt`
- ✅ Topology — queues (incl. passive), exchanges, bindings, deletes with
  guards, purge, and typed `x-*` arguments
- ✅ Producer surface — options, headers, priority, expiration, persistence,
  full AMQP message properties (`correlation_id` / `reply_to` for RPC), batch /
  multi-target publishing, and kind-based routing
- ✅ Publisher confirms (`publish_confirmed`)
- ✅ A `Codec` abstraction — JSON / string / bytes, plus `publish_encoded`
- ✅ A consumer with prefetch-bounded concurrency and per-delivery settlement
- ✅ Router-style consumer — dispatch by message `kind` to typed handlers
- ✅ Retry & dead-letter infrastructure — TTL delay queues, a DLX, and a DLQ
- ✅ A self-healing recoverable consumer — capped exponential-backoff reconnection
- ✅ A round-robin connection pool and a `Client` front door
- ✅ Config — `Config.from_env`, TLS, vhost / heartbeat / timeout, validation

## Install

```sh
opam install wren
```

Or pin this repository while it's in development:

```sh
opam pin add wren https://github.com/cargopete/wren-ocaml.git
```

## A taste

```ocaml
open Wren

let main () =
  let open Lwt.Syntax in
  (* Connect and open a channel in one step. *)
  let* client = Client.start (Config.default ()) in
  let channel = Client.channel client in

  (* Declare a queue and publish a message to it. *)
  let* _ = Connection.declare_queue channel "orders" in
  let* _ = Connection.publish_text channel ~exchange:"" ~routing_key:"orders"
             ~text:"hello, wren" in

  (* Consume from it, settling each delivery by the handler's verdict. *)
  let handler message =
    match Message.text message with
    | Some body -> print_endline ("got: " ^ body); Lwt.return Consumer.Ack
    | None -> Lwt.return Consumer.Reject
  in
  let* _consumer = Consumer.start_consumer channel "orders" handler in
  fst (Lwt.wait ())  (* run forever *)

let () = Lwt_main.run (main ())
```

## Typed messages with codecs and a router

```ocaml
open Wren

type order = { id : string }

let order_codec : order Codec.t =
  Codec.json
    ~to_json:(fun o -> `Assoc [ ("id", `String o.id) ])
    ~of_json:(function
      | `Assoc fields -> (
          match List.assoc "id" fields with
          | `String id -> { id }
          | _ -> failwith "bad id")
      | _ -> failwith "expected object")

let router =
  Router.create ()
  |> fun r ->
  Router.handle r "order.created" order_codec (fun order ->
      print_endline ("order: " ^ order.id);
      Lwt.return Consumer.Ack)
```

## Retries and dead-lettering

```ocaml
open Wren

let infra = Retry_infrastructure.for_queue "orders" (Retry.default_policy ())

(* Declares the retry exchange, DLX, main queue, DLQ, and one TTL delay queue
   per attempt, then consumes — handlers returning [Retry] are redelivered
   through the delay queues; [Dead_letter] (and exhausted retries) go to the
   DLQ. *)
let start channel handler = Consumer.start_consumer_with_retry channel handler infra
```

## Testing against a broker

A `docker-compose.yml` is provided for a local broker:

```sh
docker compose up -d
dune test
```

Integration tests honour the `RABBITMQ_*` environment variables (see
`Config.from_env`), so you can point them at any broker.

## Licence

MIT — see [LICENSE](LICENSE).

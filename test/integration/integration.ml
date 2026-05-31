(* Integration tests against a live RabbitMQ broker.

   These are deliberately {e not} part of the default `dune test` (which must run
   without a broker). Bring one up and run them explicitly:

     docker compose up -d
     dune exec test/integration/integration.exe

   Honours the RABBITMQ_* environment variables via [Config.from_env]. *)

open Wren
open Lwt.Syntax
open Lwt.Infix

let failures = ref 0
let passes = ref 0

let check name cond =
  if cond then (
    incr passes;
    Printf.printf "  [PASS] %s\n%!" name)
  else (
    incr failures;
    Printf.printf "  [FAIL] %s\n%!" name)

let or_fail label = function
  | Ok x -> Lwt.return x
  | Error e ->
      Printf.printf "  [FAIL] %s: %s\n%!" label (Errors.to_string e);
      incr failures;
      Lwt.fail (Failure label)

(* A fresh queue name per run avoids cross-test interference. *)
let qname base = Printf.sprintf "wren_it_%s_%d" base (Unix.getpid ())

(* ---- round-trip: publish then get ---- *)
let test_publish_get ch =
  let q = qname "rt" in
  let* () = Connection.declare_queue ch q >>= or_fail "declare" in
  let* () = Connection.purge_queue ch q >>= or_fail "purge" in
  let* () =
    Connection.publish_text ch ~exchange:"" ~routing_key:q ~text:"hello"
    >>= or_fail "publish"
  in
  let* body = Connection.get ch q >>= or_fail "get" in
  check "publish/get round-trip" (body = "hello");
  let* () = Connection.delete_queue ch q >>= or_fail "delete" in
  Lwt.return_unit

(* ---- consumer ack ---- *)
let test_consumer ch =
  let q = qname "consume" in
  let* () = Connection.declare_queue ch q >>= or_fail "declare" in
  let* () = Connection.purge_queue ch q >>= or_fail "purge" in
  let got = ref None in
  let waiter, wake = Lwt.wait () in
  let handler (m : Message.t) =
    got := Message.text m;
    if Lwt.state waiter = Lwt.Sleep then Lwt.wakeup_later wake ();
    Lwt.return Confirmation.Ack
  in
  let* consumer = Consumer.start_consumer ch q handler >>= or_fail "consume" in
  let* () =
    Connection.publish_text ch ~exchange:"" ~routing_key:q ~text:"delivered"
    >>= or_fail "publish"
  in
  let timeout =
    let* () = Lwt_unix.sleep 5.0 in
    Lwt.return_unit
  in
  let* () = Lwt.pick [ waiter; timeout ] in
  check "consumer received message" (!got = Some "delivered");
  let* () = Consumer.stop consumer in
  let* () = Connection.delete_queue ch q >>= or_fail "delete" in
  Lwt.return_unit

(* ---- retry -> delay queue -> redelivery -> eventual ack ---- *)
let test_retry ch =
  let base = qname "retry" in
  let policy =
    { Retry.strategy = Fixed_interval { interval_ms = 300 }; max_attempts = 3 }
  in
  let infra = Retry_infrastructure.for_queue base policy in
  let attempts = ref 0 in
  let done_waiter, wake = Lwt.wait () in
  let handler (_m : Message.t) =
    incr attempts;
    if !attempts < 2 then Lwt.return Confirmation.Retry
    else (
      if Lwt.state done_waiter = Lwt.Sleep then Lwt.wakeup_later wake ();
      Lwt.return Confirmation.Ack)
  in
  let* consumer =
    Consumer.start_consumer_with_retry ch handler infra >>= or_fail "retry consume"
  in
  let* () =
    Connection.publish_text ch ~exchange:"" ~routing_key:infra.main_queue
      ~text:"retry-me"
    >>= or_fail "publish"
  in
  let timeout =
    let* () = Lwt_unix.sleep 8.0 in
    Lwt.return_unit
  in
  let* () = Lwt.pick [ done_waiter; timeout ] in
  check "message redelivered and eventually acked" (!attempts >= 2);
  let* () = Consumer.stop consumer in
  Lwt.return_unit

let main () =
  Printf.printf "wren integration tests\n%!";
  let* client =
    Client.start (Config.from_env ()) >>= or_fail "connect (is RabbitMQ up?)"
  in
  let ch = Client.channel client in
  let* () = test_publish_get ch in
  let* () = test_consumer ch in
  let* () = test_retry ch in
  let* () = Client.close client in
  Printf.printf "\n%d passed, %d failed\n%!" !passes !failures;
  exit (if !failures = 0 then 0 else 1)

let () =
  Lwt.async_exception_hook := (fun exn ->
      Printf.eprintf "async exn: %s\n%!" (Printexc.to_string exn));
  Lwt_main.run (main ())

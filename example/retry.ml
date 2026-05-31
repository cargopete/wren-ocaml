(* Retry example: a handler that fails the first couple of attempts, then
   succeeds. Messages asking to [Retry] bounce through TTL delay queues and
   come back; exhausted ones land in the DLQ.

   Run a broker first: [docker compose up -d], then [dune exec
   example/retry.exe]. *)

open Wren
open Lwt.Syntax
open Lwt.Infix

let or_die label = function
  | Ok x -> Lwt.return x
  | Error e ->
      Printf.eprintf "%s: %s\n%!" label (Errors.to_string e);
      exit 1

let main () =
  let* client = Client.start (Config.default ()) >>= or_die "connect" in
  let ch = Client.channel client in

  (* A brisk policy so the demo doesn't dawdle: fixed 500ms, 3 attempts. *)
  let policy =
    { Retry.strategy = Fixed_interval { interval_ms = 500 }; max_attempts = 3 }
  in
  let infra = Retry_infrastructure.for_queue "jobs" policy in

  let attempts = Hashtbl.create 8 in
  let handler (message : Message.t) =
    let body = match Message.text message with Some t -> t | None -> "?" in
    let n = (try Hashtbl.find attempts body with Not_found -> 0) + 1 in
    Hashtbl.replace attempts body n;
    if n < 3 then (
      Printf.printf "attempt %d for '%s' - asking to retry\n%!" n body;
      Lwt.return Confirmation.Retry)
    else (
      Printf.printf "attempt %d for '%s' - success\n%!" n body;
      Lwt.return Confirmation.Ack)
  in

  let* _consumer =
    Consumer.start_consumer_with_retry ch handler infra >>= or_die "consume"
  in

  let* () =
    Connection.publish_text ch ~exchange:"" ~routing_key:"jobs" ~text:"job-1"
    >>= or_die "publish"
  in

  (* Wait long enough for two retry round-trips plus processing. *)
  let* () = Lwt_unix.sleep 3.0 in
  Client.close client

let () = Lwt_main.run (main ())

(* Recovery example: a self-healing consumer that owns its connection. Kill the
   broker (or the connection) and watch it reconnect with backoff and carry on.

   Run a broker first: [docker compose up -d], then [dune exec
   example/recovery.exe]. Try restarting RabbitMQ while it runs. *)

open Wren
open Lwt.Syntax
open Lwt.Infix

let or_die label = function
  | Ok x -> Lwt.return x
  | Error e ->
      Printf.eprintf "%s: %s\n%!" label (Errors.to_string e);
      exit 1

let main () =
  let config = Config.default () in

  let handler (message : Message.t) =
    Printf.printf "received: %s\n%!"
      (match Message.text message with Some t -> t | None -> "<binary>");
    Lwt.return Confirmation.Ack
  in

  let options =
    Recoverable.options ~prefetch:10
      ~on_connect:(fun _conn ->
        print_endline "wren: (re)connected; declaring topology";
        Lwt.return_unit)
      ~initial_backoff_ms:500 ~max_backoff_ms:10_000 ()
  in

  (* The consumer declares its own queue via the on_connect hook in a real app;
     here we declare it up-front with a short-lived client. *)
  let* () =
    let* client = Client.start config >>= or_die "connect" in
    let* () = Connection.declare_queue (Client.channel client) "inbox" >>= or_die "declare" in
    Client.close client
  in

  let* _consumer =
    Recoverable.start config "inbox" handler options >>= or_die "recoverable"
  in

  print_endline "recoverable consumer running on 'inbox'; Ctrl-C to stop";
  (* Run forever. *)
  fst (Lwt.wait ())

let () = Lwt_main.run (main ())

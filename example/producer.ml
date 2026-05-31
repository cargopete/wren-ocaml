(* Producer example: connect, declare a queue, and publish a few messages -
   raw, text, and a JSON-encoded typed value. Mirrors the Dart producer demo.

   Run a broker first: [docker compose up -d], then [dune exec
   example/producer.exe]. *)

open Wren
open Lwt.Syntax
open Lwt.Infix

type order = { id : string; qty : int }

let order_codec : order Codec.t =
  Codec.json
    ~to_json:(fun o -> `Assoc [ ("id", `String o.id); ("qty", `Int o.qty) ])
    ~of_json:(function
      | `Assoc fields ->
          let id =
            match List.assoc "id" fields with `String s -> s | _ -> ""
          in
          let qty =
            match List.assoc "qty" fields with `Int n -> n | _ -> 0
          in
          { id; qty }
      | _ -> failwith "expected object")

let or_die label = function
  | Ok x -> Lwt.return x
  | Error e ->
      Printf.eprintf "%s: %s\n%!" label (Errors.to_string e);
      exit 1

let main () =
  let* client = Client.start (Config.default ()) >>= or_die "connect" in
  let ch = Client.channel client in

  let* () = Connection.declare_queue ch "orders" >>= or_die "declare" in

  (* Raw text straight to the queue via the default exchange. *)
  let* () =
    Connection.publish_text ch ~exchange:"" ~routing_key:"orders"
      ~text:"hello, wren"
    >>= or_die "publish_text"
  in

  (* A typed value, JSON-encoded, stamped with a kind header. *)
  let opts =
    Publish_options.(
      create () |> (fun o -> route o "orders") |> fun o -> with_kind o "order.created")
  in
  let* () =
    Connection.publish_encoded ch { id = "A-1"; qty = 3 } order_codec opts
    >>= or_die "publish_encoded"
  in

  (* Confirm-mode publish with an explicit timeout. *)
  let* () =
    Connection.publish_confirmed ch "confirmed!"
      (Publish_options.(route (create ()) "orders"))
      2000
    >>= or_die "publish_confirmed"
  in

  print_endline "published 3 messages to 'orders'";
  Client.close client

let () = Lwt_main.run (main ())

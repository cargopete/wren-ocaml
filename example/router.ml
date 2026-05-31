(* Router example: dispatch deliveries by their [kind] header to typed handlers.
   A producer stamps each message with a kind; the router decodes and routes.

   Run a broker first: [docker compose up -d], then [dune exec
   example/router.exe]. *)

open Wren
open Lwt.Syntax
open Lwt.Infix

type order = { id : string }
type cancellation = { order_id : string; reason : string }

let order_codec : order Codec.t =
  Codec.json
    ~to_json:(fun o -> `Assoc [ ("id", `String o.id) ])
    ~of_json:(function
      | `Assoc f -> { id = (match List.assoc "id" f with `String s -> s | _ -> "") }
      | _ -> failwith "bad order")

let cancellation_codec : cancellation Codec.t =
  Codec.json
    ~to_json:(fun c ->
      `Assoc [ ("order_id", `String c.order_id); ("reason", `String c.reason) ])
    ~of_json:(function
      | `Assoc f ->
          {
            order_id = (match List.assoc "order_id" f with `String s -> s | _ -> "");
            reason = (match List.assoc "reason" f with `String s -> s | _ -> "");
          }
      | _ -> failwith "bad cancellation")

let or_die label = function
  | Ok x -> Lwt.return x
  | Error e ->
      Printf.eprintf "%s: %s\n%!" label (Errors.to_string e);
      exit 1

let main () =
  let* client = Client.start (Config.default ()) >>= or_die "connect" in
  let ch = Client.channel client in
  let* () = Connection.declare_queue ch "events" >>= or_die "declare" in

  let router =
    Router.create ()
    |> (fun r ->
         Router.handle r "order.created" order_codec (fun o ->
             Printf.printf "order created: %s\n%!" o.id;
             Lwt.return Confirmation.Ack))
    |> (fun r ->
         Router.handle r "order.cancelled" cancellation_codec (fun c ->
             Printf.printf "order %s cancelled: %s\n%!" c.order_id c.reason;
             Lwt.return Confirmation.Ack))
    |> fun r ->
    Router.fallback r (fun m ->
        Printf.printf "unrouted: %s\n%!"
          (match Message.kind m with Some k -> k | None -> "<none>");
        Lwt.return Confirmation.Reject)
  in

  let* _consumer = Consumer.start_router ch "events" router >>= or_die "consume" in

  (* Publish one of each kind. *)
  let* () =
    Connection.publish_encoded_for_kind ch (Kind_routing.create ())
      "order.created" { id = "A-1" } order_codec
      (Publish_options.route (Publish_options.create ()) "events")
    >>= or_die "publish created"
  in
  let* () =
    Connection.publish_encoded_for_kind ch (Kind_routing.create ())
      "order.cancelled"
      { order_id = "A-1"; reason = "changed mind" }
      cancellation_codec
      (Publish_options.route (Publish_options.create ()) "events")
    >>= or_die "publish cancelled"
  in

  (* Let the deliveries land, then bow out. *)
  let* () = Lwt_unix.sleep 1.0 in
  Client.close client

let () = Lwt_main.run (main ())

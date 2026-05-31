(* A self-healing consumer that owns its own connection. If the connection
   drops, it reconnects with capped exponential backoff, re-opening the channel,
   re-declaring any retry topology, and re-subscribing.

   Where the Gleam port leans on OTP monitoring, this uses the connection's
   [on_closed] promise as the drop signal and an explicit backoff loop - the
   same shape as the Dart port's recovery loop. *)

open Lwt.Syntax

type options = {
  prefetch : int option;
  retry : Retry_infrastructure.t option;
  on_connect : Connection.t -> unit Lwt.t;
  initial_backoff_ms : int;
  max_backoff_ms : int;
  max_concurrent : int;
  consume : Topology.consume_options;
}

let options ?prefetch ?retry ?(on_connect = fun _ -> Lwt.return_unit)
    ?(initial_backoff_ms = 500) ?(max_backoff_ms = 30_000) ?(max_concurrent = 1)
    ?(consume = Topology.consume_options ()) () =
  {
    prefetch;
    retry;
    on_connect;
    initial_backoff_ms;
    max_backoff_ms;
    max_concurrent = max max_concurrent 1;
    consume;
  }

type t = { stop : unit -> unit Lwt.t }

let stop t = t.stop ()

(* Connect, configure the channel, (re)declare retry topology, subscribe, and
   return the live connection/channel/consumer triple. *)
let establish config queue handler opts =
  let open Lwt_result.Syntax in
  let* connection = Connection.connect config in
  let* channel = Connection.open_channel connection in
  (* An explicit prefetch wins; otherwise concurrency implies a matching one. *)
  let prefetch =
    match opts.prefetch with
    | Some count -> Some count
    | None -> if opts.max_concurrent > 1 then Some opts.max_concurrent else None
  in
  let* () =
    match prefetch with
    | Some count -> Connection.qos channel count
    | None -> Lwt_result.return ()
  in
  let* () =
    match opts.retry with
    | Some infra -> Consumer.setup_retry channel infra
    | None -> Lwt_result.return ()
  in
  let subscribe_queue =
    match opts.retry with Some infra -> infra.main_queue | None -> queue
  in
  let* consumer =
    Consumer.start_internal channel ~queue:subscribe_queue ~handler
      ~retry:opts.retry ~concurrency:opts.max_concurrent ~consume:opts.consume
  in
  let* () = Lwt_result.ok (opts.on_connect connection) in
  Lwt_result.return (connection, channel, consumer)

let start config queue handler opts =
  let stopped = ref false in
  (* Loop: establish, wait for the connection to drop (or stop), then retry
     with capped exponential backoff. *)
  let rec run backoff_ms =
    if !stopped then Lwt.return_unit
    else
      let* established = establish config queue handler opts in
      match established with
      | Ok (connection, _channel, _consumer) ->
          (* Reset backoff after a successful connect, then wait for the drop. *)
          let* () = Connection.on_closed connection in
          if !stopped then Lwt.return_unit
          else (
            Log.warning "connection lost; reconnecting...";
            run opts.initial_backoff_ms)
      | Error _ ->
          let* () = Lwt_unix.sleep (float_of_int backoff_ms /. 1000.) in
          if !stopped then Lwt.return_unit
          else
            let next = min (backoff_ms * 2) opts.max_backoff_ms in
            Log.warning
              (Printf.sprintf "reconnect failed; retrying in %dms" next);
            run next
  in
  (* Kick off establishment; report the first attempt's outcome to the caller. *)
  let* first = establish config queue handler opts in
  match first with
  | Error e -> Lwt.return (Error e)
  | Ok (connection, _channel, _consumer) ->
      Lwt.async (fun () ->
          let* () = Connection.on_closed connection in
          if !stopped then Lwt.return_unit
          else (
            Log.warning "connection lost; reconnecting...";
            run opts.initial_backoff_ms));
      let stop () =
        stopped := true;
        Connection.close_connection connection
      in
      Lwt.return (Ok { stop })

let start_router config queue router opts =
  start config queue (fun message -> Router.dispatch router message) opts

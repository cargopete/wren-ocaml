(* A subscription that drains a delivery pipe, runs a handler per message, and
   settles each delivery with the broker according to the handler's verdict.
   Retry / dead-letter verdicts are routed through delay-queue infrastructure
   (see {!Retry_infrastructure} and {!setup_retry}). *)

module Amqp = Amqp_client_lwt
open Lwt.Syntax

(* Re-export the shared verdict so [Consumer.Ack] etc. read naturally. *)
type confirmation = Confirmation.t = Ack | Reject | Retry | Dead_letter
type handler = Confirmation.handler

(* Silence "unused constructor" warnings: the constructors exist purely to be
   re-exported through the .mli for callers' convenience. *)
let _ = [ Ack; Reject; Retry; Dead_letter ]

type t = { cancel : unit -> unit Lwt.t }

let stop c = c.cancel ()

let dlq_routing_key = Retry_infrastructure.dlq_routing_key

(* ----------------------------------------------------------------------- *)
(* Retry topology declaration                                              *)
(* ----------------------------------------------------------------------- *)

let setup_retry ch (infra : Retry_infrastructure.t) =
  let open Lwt_result.Syntax in
  let exo = Topology.exchange_options () in
  let* () = Connection.declare_exchange ch infra.retry_exchange Topology.Direct exo in
  let* () = Connection.declare_exchange ch infra.dlx_exchange Topology.Direct exo in
  let* () = Connection.declare_queue ch infra.main_queue in
  let* () = Connection.declare_queue ch infra.dlq in
  let* () =
    Connection.bind_queue ch ~queue:infra.dlq ~exchange:infra.dlx_exchange
      ~routing_key:dlq_routing_key
  in
  let rec slots = function
    | [] -> Lwt_result.return ()
    | (queue, routing_key, ttl) :: rest ->
        let qopts =
          Topology.queue_options
            ~arguments:
              [
                ("x-message-ttl", Topology.Int_arg ttl);
                ("x-dead-letter-exchange", Topology.String_arg "");
                ("x-dead-letter-routing-key", Topology.String_arg infra.main_queue);
              ]
            ()
        in
        let* () = Connection.declare_queue_with ch queue qopts in
        let* () =
          Connection.bind_queue ch ~queue ~exchange:infra.retry_exchange
            ~routing_key
        in
        slots rest
  in
  slots (Retry_infrastructure.slots infra)

(* ----------------------------------------------------------------------- *)
(* Settlement                                                              *)
(* ----------------------------------------------------------------------- *)

let ack ch raw =
  Lwt.catch
    (fun () -> Amqp.Message.ack (Connection.amqp_channel ch) raw)
    (fun _ -> Lwt.return_unit)

let reject ch raw =
  Lwt.catch
    (fun () -> Amqp.Message.reject ~requeue:false (Connection.amqp_channel ch) raw)
    (fun _ -> Lwt.return_unit)

(* Timestamp a failure: always set [last_retry]; set [first_death] only once. *)
let stamp (m : Retry.metadata) =
  let now = Clock.now_rfc3339 () in
  let first_death =
    match m.first_death with Some _ -> m.first_death | None -> Some now
  in
  { m with last_retry = Some now; first_death }

(* Overlay refreshed retry headers onto the message's existing headers,
   replacing any stale retry headers of the same name. *)
let merge_retry_headers original metadata =
  let refreshed = Retry.to_headers metadata in
  let refreshed_keys = List.map fst refreshed in
  let kept =
    List.filter (fun (k, _) -> not (List.mem k refreshed_keys)) original
  in
  kept @ refreshed

let reroute ch (message : Message.t) raw ~exchange ~routing_key metadata =
  let options =
    Publish_options.(
      create ()
      |> (fun o -> to_exchange o exchange)
      |> (fun o -> route o routing_key)
      |> fun o -> with_headers o (merge_retry_headers message.headers metadata))
  in
  let* result = Connection.publish_with_options ch message.payload options in
  match result with
  | Ok () -> ack ch raw
  | Error _ ->
      Log.warning ("failed to route message to '" ^ exchange ^ "'");
      reject ch raw

let settle_retry ch infra (message : Message.t) raw =
  match infra with
  | None ->
      Log.warning
        "Retry requested but no retry infrastructure configured; rejecting";
      reject ch raw
  | Some (infra : Retry_infrastructure.t) ->
      let metadata =
        stamp
          (Retry.record_failure
             (Retry.from_headers message.headers infra.policy.max_attempts)
             "handler returned Retry")
      in
      let exchange, routing_key =
        if Retry.is_exhausted metadata then (infra.dlx_exchange, dlq_routing_key)
        else
          ( infra.retry_exchange,
            Retry_infrastructure.routing_key_for_attempt infra metadata.attempt )
      in
      reroute ch message raw ~exchange ~routing_key metadata

let settle_dead_letter ch infra (message : Message.t) raw =
  match infra with
  | None ->
      (* No DLQ to route to - reject without requeue (the broker's own DLX,
         if any, then applies). *)
      reject ch raw
  | Some (infra : Retry_infrastructure.t) ->
      let metadata =
        stamp
          (Retry.record_failure
             (Retry.from_headers message.headers infra.policy.max_attempts)
             "handler returned DeadLetter")
      in
      reroute ch message raw ~exchange:infra.dlx_exchange
        ~routing_key:dlq_routing_key metadata

let settle ch infra (message : Message.t) raw confirmation =
  match confirmation with
  | Confirmation.Ack -> ack ch raw
  | Confirmation.Reject -> reject ch raw
  | Confirmation.Retry -> settle_retry ch infra message raw
  | Confirmation.Dead_letter -> settle_dead_letter ch infra message raw

(* ----------------------------------------------------------------------- *)
(* Delivery processing                                                     *)
(* ----------------------------------------------------------------------- *)

let process ch infra (handler : handler) ~auto_ack raw =
  Lwt.catch
    (fun () ->
      let message = Connection.message_of_amqp raw in
      let* confirmation = handler message in
      if auto_ack then Lwt.return_unit else settle ch infra message raw confirmation)
    (fun exn ->
      (* A crashing handler must not take the consumer down; leave the message
         unacked so the broker redelivers it. *)
      Log.warning ("handler raised: " ^ Printexc.to_string exn);
      Lwt.return_unit)

(* ----------------------------------------------------------------------- *)
(* Starting consumers                                                      *)
(* ----------------------------------------------------------------------- *)

let start_internal ch ~queue ~handler ~retry ~concurrency
    ~(consume : Topology.consume_options) =
  Lwt.catch
    (fun () ->
      let* () =
        if concurrency > 1 then
          let* _ = Connection.qos ch concurrency in
          Lwt.return_unit
        else Lwt.return_unit
      in
      let raw_chan = Connection.amqp_channel ch in
      let* q = Amqp.Queue.fake raw_chan queue in
      let* consumer, reader =
        Amqp.Queue.consume ~id:"wren" ~no_local:consume.no_local
          ~no_ack:consume.auto_ack ~exclusive:consume.exclusive
          ~on_cancel:(fun () -> ()) raw_chan q
      in
      let auto_ack = consume.auto_ack in
      let f raw =
        if concurrency > 1 then (
          (* Process concurrently; the broker's prefetch bounds in-flight work. *)
          Lwt.async (fun () -> process ch retry handler ~auto_ack raw);
          Lwt.return_unit)
        else process ch retry handler ~auto_ack raw
      in
      Lwt.async (fun () -> Amqp.Thread.Pipe.iter reader ~f);
      let cancel () =
        Lwt.catch
          (fun () -> Amqp.Queue.cancel consumer)
          (fun _ -> Lwt.return_unit)
      in
      Lwt.return (Ok { cancel }))
    (fun exn ->
      Lwt.return (Error (Errors.Channel_failed (Printexc.to_string exn))))

let start_consumer ch queue handler =
  start_internal ch ~queue ~handler ~retry:None ~concurrency:1
    ~consume:(Topology.consume_options ())

let start_consumer_with_options ch queue handler options =
  start_internal ch ~queue ~handler ~retry:None ~concurrency:1 ~consume:options

let start_consumer_concurrent ch queue handler max_concurrent =
  start_internal ch ~queue ~handler ~retry:None
    ~concurrency:(max max_concurrent 1)
    ~consume:(Topology.consume_options ())

let start_consumer_with_retry ch handler (infra : Retry_infrastructure.t) =
  let* setup = setup_retry ch infra in
  match setup with
  | Error e -> Lwt.return (Error e)
  | Ok () ->
      start_internal ch ~queue:infra.main_queue ~handler ~retry:(Some infra)
        ~concurrency:1 ~consume:(Topology.consume_options ())

(* ----------------------------------------------------------------------- *)
(* Router-backed consumers                                                 *)
(* ----------------------------------------------------------------------- *)

let start_router ch queue router =
  start_consumer ch queue (fun message -> Router.dispatch router message)

let start_router_with_retry ch router infra =
  start_consumer_with_retry ch (fun message -> Router.dispatch router message) infra

(* The IO core: connection & channel lifecycle, topology, and publishing, over
   amqp-client-lwt. The underlying library is strongly, phantom-typed (channels
   by confirm-mode, exchanges by routing-key shape); wren's surface is
   name-and-string based, so this module bridges the two:

   - every channel is opened [with_confirm], so publishes await a broker ack;
   - exchange handles are cached by name and packed in a GADT so a bind can
     recover the exact routing-key argument the library demands. *)

module Amqp = Amqp_client_lwt
module Types = Amqp_client_lwt.Amqp.Types
module Content = Amqp.Spec.Basic.Content
open Lwt.Syntax

type t = { conn : Amqp.Connection.t }

(* An exchange handle whose phantom type is recovered by matching. This lets a
   heterogeneous cache hold direct/fanout/topic/headers handles together, while
   [bind] still produces the correctly-typed routing-key argument. *)
type packed_exchange =
  | Pack_direct of [ `Queue of string ] Amqp.Exchange.t
  | Pack_fanout of unit Amqp.Exchange.t
  | Pack_topic of [ `Topic of string ] Amqp.Exchange.t
  | Pack_match of [ `Headers of Types.header list ] Amqp.Exchange.t

type channel = {
  chan : Amqp.Channel.with_confirm Amqp.Channel.t;
  exchanges : (string, packed_exchange) Hashtbl.t;
}

(* ----------------------------------------------------------------------- *)
(* Error plumbing                                                          *)
(* ----------------------------------------------------------------------- *)

(* amqp-client-lwt signals failures by raising, so each operation is wrapped
   and the exception turned into a wren [result]. *)
let guard err f =
  Lwt.catch
    (fun () -> Lwt.map (fun x -> Ok x) (f ()))
    (fun exn -> Lwt.return (Error (err (Printexc.to_string exn))))

let chan_guard f = guard (fun r -> Errors.Channel_failed r) f

(* ----------------------------------------------------------------------- *)
(* Argument / header conversions                                           *)
(* ----------------------------------------------------------------------- *)

let value_of_arg : Topology.arg -> Types.value = function
  | Topology.Int_arg n -> Types.VLonglong n
  | Topology.String_arg s -> Types.VLongstr s
  | Topology.Bool_arg b -> Types.VBoolean b

let table_of_args args = List.map (fun (k, a) -> (k, value_of_arg a)) args

let table_of_headers headers =
  List.map (fun (k, v) -> (k, Types.VLongstr v)) headers

(* ----------------------------------------------------------------------- *)
(* Connection & channel lifecycle                                          *)
(* ----------------------------------------------------------------------- *)

let connect (config : Config.t) =
  Lwt.catch
    (fun () ->
      let connecting =
        let* conn =
          Amqp.Connection.connect ~id:"wren"
            ~virtual_host:config.virtual_host ~port:config.port
            ~credentials:(config.username, config.password)
            ~heartbeat:config.heartbeat_seconds config.host
        in
        Lwt.return (`Ok conn)
      in
      let timeout =
        let* () =
          Lwt_unix.sleep (float_of_int config.connection_timeout_ms /. 1000.)
        in
        Lwt.return `Timeout
      in
      let* outcome = Lwt.pick [ connecting; timeout ] in
      match outcome with
      | `Ok conn -> Lwt.return (Ok { conn })
      | `Timeout ->
          Lwt.return (Error (Errors.Connection_failed "connection timed out")))
    (fun exn ->
      Lwt.return (Error (Errors.Connection_failed (Printexc.to_string exn))))

let open_channel t =
  guard
    (fun r -> Errors.Channel_failed r)
    (fun () ->
      let* chan =
        Amqp.Connection.open_channel ~id:"wren" Amqp.Channel.with_confirm t.conn
      in
      Lwt.return { chan; exchanges = Hashtbl.create 8 })

(* A cheap liveness check: the connection's [on_closed] promise is still
   pending while it is open. *)
let is_open t =
  match Lwt.state (Amqp.Connection.on_closed t.conn) with
  | Lwt.Sleep -> true
  | _ -> false

let close_connection t =
  Lwt.catch
    (fun () -> Amqp.Connection.close t.conn)
    (fun _ -> Lwt.return_unit)

let close_channel ch =
  Lwt.catch (fun () -> Amqp.Channel.close ch.chan) (fun _ -> Lwt.return_unit)

(* Active probe: a server-named, exclusive, auto-delete queue round-trips to
   the broker, proving the channel is responsive (not merely alive). *)
let health_check ch =
  chan_guard (fun () ->
      let* _q =
        Amqp.Queue.declare ch.chan ~exclusive:true ~auto_delete:true
          ~autogenerate:true ""
      in
      Lwt.return_unit)

let qos_with ch ~prefetch_count ~prefetch_size ~global =
  chan_guard (fun () ->
      if global then
        Amqp.Channel.set_global_prefetch ~count:prefetch_count
          ~size:prefetch_size ch.chan
      else
        Amqp.Channel.set_prefetch ~count:prefetch_count ~size:prefetch_size
          ch.chan)

let qos ch prefetch_count =
  qos_with ch ~prefetch_count ~prefetch_size:0 ~global:false

(* ----------------------------------------------------------------------- *)
(* Topology — queues                                                       *)
(* ----------------------------------------------------------------------- *)

let declare_queue_with ch name (options : Topology.queue_options) =
  chan_guard (fun () ->
      let* _q =
        Amqp.Queue.declare ch.chan ~durable:options.durable
          ~exclusive:options.exclusive ~auto_delete:options.auto_delete
          ~arguments:(table_of_args options.arguments)
          name
      in
      Lwt.return_unit)

let declare_queue ch name =
  declare_queue_with ch name (Topology.queue_options ())

let declare_queue_passive ch name =
  chan_guard (fun () ->
      let* _q = Amqp.Queue.declare ch.chan ~passive:true name in
      Lwt.return_unit)

let purge_queue ch name =
  chan_guard (fun () ->
      let* q = Amqp.Queue.fake ch.chan name in
      Amqp.Queue.purge ch.chan q)

let delete_queue_with ch name ~if_unused ~if_empty =
  chan_guard (fun () ->
      let* q = Amqp.Queue.fake ch.chan name in
      Amqp.Queue.delete ~if_unused ~if_empty ch.chan q)

let delete_queue ch name =
  delete_queue_with ch name ~if_unused:false ~if_empty:false

(* ----------------------------------------------------------------------- *)
(* Topology — exchanges                                                    *)
(* ----------------------------------------------------------------------- *)

let declare_exchange ch name kind (options : Topology.exchange_options) =
  chan_guard (fun () ->
      let args = table_of_args options.arguments in
      let declare exchange_type =
        Amqp.Exchange.declare ~durable:options.durable
          ~auto_delete:options.auto_delete ~internal:options.internal ch.chan
          exchange_type ~arguments:args name
      in
      let* packed =
        match kind with
        | Topology.Direct ->
            let* e = declare Amqp.Exchange.direct_t in
            Lwt.return (Pack_direct e)
        | Topology.Fanout ->
            let* e = declare Amqp.Exchange.fanout_t in
            Lwt.return (Pack_fanout e)
        | Topology.Topic ->
            let* e = declare Amqp.Exchange.topic_t in
            Lwt.return (Pack_topic e)
        | Topology.Headers ->
            let* e = declare Amqp.Exchange.match_t in
            Lwt.return (Pack_match e)
      in
      Hashtbl.replace ch.exchanges name packed;
      Lwt.return_unit)

(* Resolve a handle for an exchange name: the default exchange, a cached
   handle, or a best-effort passive declare (as direct) for an exchange
   declared elsewhere. A failed passive declare closes the channel - the same
   caveat as the Gleam port. *)
let exchange_handle ch name =
  if name = "" then Lwt.return (Pack_direct Amqp.Exchange.default)
  else
    match Hashtbl.find_opt ch.exchanges name with
    | Some packed -> Lwt.return packed
    | None ->
        let* e =
          Amqp.Exchange.declare ~passive:true ch.chan Amqp.Exchange.direct_t name
        in
        let packed = Pack_direct e in
        Hashtbl.replace ch.exchanges name packed;
        Lwt.return packed

let delete_exchange_with ch name ~if_unused =
  chan_guard (fun () ->
      let* packed = exchange_handle ch name in
      let* () =
        match packed with
        | Pack_direct e -> Amqp.Exchange.delete ~if_unused ch.chan e
        | Pack_fanout e -> Amqp.Exchange.delete ~if_unused ch.chan e
        | Pack_topic e -> Amqp.Exchange.delete ~if_unused ch.chan e
        | Pack_match e -> Amqp.Exchange.delete ~if_unused ch.chan e
      in
      Hashtbl.remove ch.exchanges name;
      Lwt.return_unit)

let delete_exchange ch name = delete_exchange_with ch name ~if_unused:false

(* ----------------------------------------------------------------------- *)
(* Bindings                                                                *)
(* ----------------------------------------------------------------------- *)

let bind_queue_with ch ~queue ~exchange ~routing_key ~arguments =
  chan_guard (fun () ->
      let* q = Amqp.Queue.fake ch.chan queue in
      let* packed = exchange_handle ch exchange in
      match packed with
      | Pack_direct e -> Amqp.Queue.bind ch.chan q e (`Queue routing_key)
      | Pack_fanout e -> Amqp.Queue.bind ch.chan q e ()
      | Pack_topic e -> Amqp.Queue.bind ch.chan q e (`Topic routing_key)
      | Pack_match e ->
          Amqp.Queue.bind ch.chan q e (`Headers (table_of_args arguments)))

let bind_queue ch ~queue ~exchange ~routing_key =
  bind_queue_with ch ~queue ~exchange ~routing_key ~arguments:[]

let unbind_queue ch ~queue ~exchange ~routing_key =
  chan_guard (fun () ->
      let* q = Amqp.Queue.fake ch.chan queue in
      let* packed = exchange_handle ch exchange in
      match packed with
      | Pack_direct e -> Amqp.Queue.unbind ch.chan q e (`Queue routing_key)
      | Pack_fanout e -> Amqp.Queue.unbind ch.chan q e ()
      | Pack_topic e -> Amqp.Queue.unbind ch.chan q e (`Topic routing_key)
      | Pack_match e -> Amqp.Queue.unbind ch.chan q e (`Headers []))

(* ----------------------------------------------------------------------- *)
(* Building AMQP messages from publish options                             *)
(* ----------------------------------------------------------------------- *)

let find_prop options f = List.find_map f options.Publish_options.properties

let build_message (options : Publish_options.t) payload =
  let correlation_id =
    find_prop options (function
      | Publish_options.Correlation_id v -> Some v
      | _ -> None)
  in
  let reply_to =
    find_prop options (function Publish_options.Reply_to v -> Some v | _ -> None)
  in
  let message_id =
    find_prop options (function
      | Publish_options.Message_id v -> Some v
      | _ -> None)
  in
  let amqp_type =
    find_prop options (function
      | Publish_options.Message_type v -> Some v
      | _ -> None)
  in
  let user_id =
    find_prop options (function Publish_options.User_id v -> Some v | _ -> None)
  in
  let app_id =
    find_prop options (function Publish_options.App_id v -> Some v | _ -> None)
  in
  let content_encoding =
    find_prop options (function
      | Publish_options.Content_encoding v -> Some v
      | _ -> None)
  in
  let timestamp =
    find_prop options (function
      | Publish_options.Timestamp v -> Some v
      | _ -> None)
  in
  let headers =
    match options.headers with [] -> None | h -> Some (table_of_headers h)
  in
  let delivery_mode = if options.persistent then Some 2 else None in
  Amqp.Message.make ?content_type:options.content_type ?content_encoding
    ?headers ?delivery_mode ?priority:options.priority ?correlation_id
    ?reply_to ?expiration:options.expiration ?message_id ?timestamp ?amqp_type
    ?user_id ?app_id payload

let publish_packed ch packed ~routing_key ~mandatory message =
  match packed with
  | Pack_direct e -> Amqp.Exchange.publish ch.chan e ~mandatory ~routing_key message
  | Pack_fanout e -> Amqp.Exchange.publish ch.chan e ~mandatory ~routing_key message
  | Pack_topic e -> Amqp.Exchange.publish ch.chan e ~mandatory ~routing_key message
  | Pack_match e -> Amqp.Exchange.publish ch.chan e ~mandatory ~routing_key message

(* ----------------------------------------------------------------------- *)
(* Publishing                                                              *)
(* ----------------------------------------------------------------------- *)

let publish_with_options ch payload (options : Publish_options.t) =
  Lwt.catch
    (fun () ->
      let message = build_message options payload in
      let* packed = exchange_handle ch options.exchange in
      let* result =
        publish_packed ch packed ~routing_key:options.routing_key
          ~mandatory:options.mandatory message
      in
      match result with
      | `Ok -> Lwt.return (Ok ())
      | `Failed ->
          Lwt.return
            (Error (Errors.Channel_failed "publish was not confirmed by the broker")))
    (fun exn ->
      Lwt.return (Error (Errors.Channel_failed (Printexc.to_string exn))))

let publish ch ~exchange ~routing_key ~payload =
  let options =
    Publish_options.(route (to_exchange (create ()) exchange) routing_key)
  in
  publish_with_options ch payload options

let publish_text ch ~exchange ~routing_key ~text =
  publish ch ~exchange ~routing_key ~payload:text

(* The channel is already in confirm mode, so this is a documented no-op kept
   for API parity with the Gleam / Dart ports. *)
let enable_confirms _ch = Lwt.return (Ok ())

let publish_confirmed ch payload options timeout_ms =
  let publishing =
    let* r = publish_with_options ch payload options in
    Lwt.return (`Done r)
  in
  let timeout =
    let* () = Lwt_unix.sleep (float_of_int timeout_ms /. 1000.) in
    Lwt.return `Timeout
  in
  let* outcome = Lwt.pick [ publishing; timeout ] in
  match outcome with
  | `Done r -> Lwt.return r
  | `Timeout ->
      Lwt.return (Error (Errors.Channel_failed "publish confirmation timed out"))

let get ?(timeout_ms = 1000) ch queue =
  chan_guard (fun () ->
      let* q = Amqp.Queue.fake ch.chan queue in
      let retries = max 1 (timeout_ms / 100) in
      let rec loop n =
        let* m = Amqp.Queue.get ~no_ack:true ch.chan q in
        match m with
        | Some msg ->
            let _content, body = msg.Amqp.Message.message in
            Lwt.return body
        | None ->
            if n <= 1 then Lwt.fail (Failure "no message available")
            else
              let* () = Lwt_unix.sleep 0.1 in
              loop (n - 1)
      in
      loop retries)

(* ----------------------------------------------------------------------- *)
(* Batch / multi-target publishing                                         *)
(* ----------------------------------------------------------------------- *)

let publish_to_target ch (target : Publish_options.target) payload
    (options : Publish_options.t) =
  let options =
    { options with exchange = target.exchange; routing_key = target.routing_key }
  in
  publish_with_options ch payload options

let publish_batch ch messages options =
  let* failures =
    Lwt_list.fold_left_s
      (fun acc (target, payload) ->
        let* result = publish_to_target ch target payload options in
        match result with
        | Ok () -> Lwt.return acc
        | Error error ->
            Lwt.return
              ({ Publish_options.target; payload; error } :: acc))
      [] messages
  in
  let failures = List.rev failures in
  Lwt.return
    {
      Publish_options.published = List.length messages - List.length failures;
      failures;
    }

let publish_to_targets ch payload targets options =
  publish_batch ch (List.map (fun target -> (target, payload)) targets) options

let publish_batch_with_retry ch messages options max_attempts =
  let rec retry messages attempts_left published_so_far =
    let* result = publish_batch ch messages options in
    let published = published_so_far + result.Publish_options.published in
    match (result.Publish_options.failures, attempts_left <= 1) with
    | [], _ -> Lwt.return { Publish_options.published; failures = [] }
    | failures, true -> Lwt.return { Publish_options.published; failures }
    | failures, false ->
        retry
          (List.map
             (fun (f : Publish_options.batch_failure) -> (f.target, f.payload))
             failures)
          (attempts_left - 1) published
  in
  retry messages (max max_attempts 1) 0

(* ----------------------------------------------------------------------- *)
(* Encoded & kind-routed publishing                                        *)
(* ----------------------------------------------------------------------- *)

let publish_encoded ch value codec options =
  match Codec.encode codec value with
  | Ok payload -> publish_with_options ch payload options
  | Error error ->
      Lwt.return (Error (Errors.Encoding_failed (Codec.error_reason error)))

let publish_for_kind ch routing kind payload options =
  let options = Publish_options.with_kind (Kind_routing.apply routing kind options) kind in
  publish_with_options ch payload options

let publish_encoded_for_kind ch routing kind value codec options =
  match Codec.encode codec value with
  | Ok payload -> publish_for_kind ch routing kind payload options
  | Error error ->
      Lwt.return (Error (Errors.Encoding_failed (Codec.error_reason error)))

(* ----------------------------------------------------------------------- *)
(* Bridging deliveries to wren messages (used by the consumer)             *)
(* ----------------------------------------------------------------------- *)

let message_of_amqp (m : Amqp.Message.t) : Message.t =
  let content, body = m.Amqp.Message.message in
  let headers =
    match content.Content.headers with
    | None -> []
    | Some table ->
        List.filter_map
          (fun (k, v) ->
            match v with
            | Types.VLongstr s | Types.VShortstr s -> Some (k, s)
            | _ -> None)
          table
  in
  {
    Message.payload = body;
    routing_key = m.Amqp.Message.routing_key;
    headers;
    correlation_id = content.Content.correlation_id;
    reply_to = content.Content.reply_to;
    redelivered = m.Amqp.Message.redelivered;
  }

(* Low-level accessors for advanced use / sibling modules. *)
let amqp_channel ch = ch.chan
let amqp_connection t = t.conn

(* A promise that resolves when the connection closes - the drop signal the
   recoverable consumer waits on. *)
let on_closed t = Amqp.Connection.on_closed t.conn

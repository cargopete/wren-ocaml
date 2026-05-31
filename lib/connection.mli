(** Connection & channel lifecycle, topology declaration, and publishing.

    A thin, name-and-string layer over [amqp-client-lwt]. Every channel is opened
    in publisher-confirm mode, so {!publish_with_options} (and friends) await a
    broker acknowledgement and report a nack as an error. *)

(** An open connection to a RabbitMQ broker. *)
type t

(** A channel multiplexed over a {!t}. Holds a cache of declared exchange
    handles so binding and publishing can recover the broker's exact routing-key
    requirements. *)
type channel

(** {2 Connection & channel lifecycle} *)

(** Open a connection to the broker. Honours [config.connection_timeout_ms]. *)
val connect : Config.t -> (t, Errors.t) result Lwt.t

(** Open a channel over an existing connection (in confirm mode). *)
val open_channel : t -> (channel, Errors.t) result Lwt.t

(** A cheap liveness check (the connection has not closed). *)
val is_open : t -> bool

(** A promise that resolves when the connection closes. *)
val on_closed : t -> unit Lwt.t

(** Close a connection (and implicitly its channels). Safe if already closed. *)
val close_connection : t -> unit Lwt.t

(** Close a channel. Safe to call even if already closed. *)
val close_channel : channel -> unit Lwt.t

(** Actively probe a channel by round-tripping a throwaway queue declare. *)
val health_check : channel -> (unit, Errors.t) result Lwt.t

(** Set channel prefetch: the number of unacknowledged messages the broker will
    deliver before waiting for acks. *)
val qos : channel -> int -> (unit, Errors.t) result Lwt.t

(** Set prefetch with full control over [prefetch_size] (octets, [0] for no
    limit) and whether the setting is [global] (channel-wide vs per-consumer). *)
val qos_with :
  channel ->
  prefetch_count:int ->
  prefetch_size:int ->
  global:bool ->
  (unit, Errors.t) result Lwt.t

(** {2 Topology — queues} *)

val declare_queue : channel -> string -> (unit, Errors.t) result Lwt.t

(** Check that a queue exists without creating it (a passive declare). Errors if
    it doesn't exist. Note: a failed passive declare closes the channel. *)
val declare_queue_passive : channel -> string -> (unit, Errors.t) result Lwt.t

val declare_queue_with :
  channel -> string -> Topology.queue_options -> (unit, Errors.t) result Lwt.t

val purge_queue : channel -> string -> (unit, Errors.t) result Lwt.t
val delete_queue : channel -> string -> (unit, Errors.t) result Lwt.t

val delete_queue_with :
  channel ->
  string ->
  if_unused:bool ->
  if_empty:bool ->
  (unit, Errors.t) result Lwt.t

(** {2 Topology — exchanges} *)

(** Declare an exchange of the given kind (idempotent). The handle is cached on
    the channel for later binding/publishing. *)
val declare_exchange :
  channel ->
  string ->
  Topology.exchange_kind ->
  Topology.exchange_options ->
  (unit, Errors.t) result Lwt.t

val delete_exchange : channel -> string -> (unit, Errors.t) result Lwt.t

val delete_exchange_with :
  channel -> string -> if_unused:bool -> (unit, Errors.t) result Lwt.t

(** {2 Bindings} *)

val bind_queue :
  channel ->
  queue:string ->
  exchange:string ->
  routing_key:string ->
  (unit, Errors.t) result Lwt.t

(** Bind with binding arguments - needed for [Headers] exchanges, where the
    [x-match] argument and header matchers live on the binding. *)
val bind_queue_with :
  channel ->
  queue:string ->
  exchange:string ->
  routing_key:string ->
  arguments:(string * Topology.arg) list ->
  (unit, Errors.t) result Lwt.t

val unbind_queue :
  channel ->
  queue:string ->
  exchange:string ->
  routing_key:string ->
  (unit, Errors.t) result Lwt.t

(** {2 Publishing} *)

(** Publish raw bytes to an exchange with a routing key. Use [""] as the
    exchange to publish straight to a queue by name. *)
val publish :
  channel ->
  exchange:string ->
  routing_key:string ->
  payload:string ->
  (unit, Errors.t) result Lwt.t

(** Publish a UTF-8 [text] payload - the common case. *)
val publish_text :
  channel ->
  exchange:string ->
  routing_key:string ->
  text:string ->
  (unit, Errors.t) result Lwt.t

(** Publish a message with the full set of {!Publish_options.t}. *)
val publish_with_options :
  channel -> string -> Publish_options.t -> (unit, Errors.t) result Lwt.t

(** Put a channel into publisher-confirm mode. A documented no-op here (channels
    are always in confirm mode), kept for parity with the Gleam / Dart ports. *)
val enable_confirms : channel -> (unit, Errors.t) result Lwt.t

(** Publish and wait (up to [timeout_ms]) for the broker to confirm. *)
val publish_confirmed :
  channel -> string -> Publish_options.t -> int -> (unit, Errors.t) result Lwt.t

(** Fetch a single message's raw bytes from a queue (polls briefly, up to
    [timeout_ms]). A primitive for one-off fetches; prefer a consumer for
    ongoing work. *)
val get : ?timeout_ms:int -> channel -> string -> (string, Errors.t) result Lwt.t

(** {2 Batch / multi-target publishing} *)

(** Publish each [(target, payload)] in turn (order preserved), collecting
    per-message failures rather than stopping at the first. *)
val publish_batch :
  channel ->
  (Publish_options.target * string) list ->
  Publish_options.t ->
  Publish_options.batch_result Lwt.t

(** Publish one [payload] to several targets. *)
val publish_to_targets :
  channel ->
  string ->
  Publish_options.target list ->
  Publish_options.t ->
  Publish_options.batch_result Lwt.t

(** Like {!publish_batch}, but re-publishes failures up to [max_attempts] times. *)
val publish_batch_with_retry :
  channel ->
  (Publish_options.target * string) list ->
  Publish_options.t ->
  int ->
  Publish_options.batch_result Lwt.t

(** {2 Encoded & kind-routed publishing} *)

(** Encode a typed [value] with [codec] and publish it with the given options. *)
val publish_encoded :
  channel ->
  'a ->
  'a Codec.t ->
  Publish_options.t ->
  (unit, Errors.t) result Lwt.t

(** Publish [payload] for [kind], applying the routing table and stamping the
    [kind] header. *)
val publish_for_kind :
  channel ->
  Kind_routing.t ->
  string ->
  string ->
  Publish_options.t ->
  (unit, Errors.t) result Lwt.t

(** Like {!publish_for_kind}, but encodes a typed value with [codec] first. *)
val publish_encoded_for_kind :
  channel ->
  Kind_routing.t ->
  string ->
  'a ->
  'a Codec.t ->
  Publish_options.t ->
  (unit, Errors.t) result Lwt.t

(**/**)

(* Internal accessors used by sibling modules ({!Consumer}, {!Recoverable}). Not
   part of the stable public API. *)

val amqp_channel :
  channel -> Amqp_client_lwt.Channel.with_confirm Amqp_client_lwt.Channel.t

val amqp_connection : t -> Amqp_client_lwt.Connection.t
val message_of_amqp : Amqp_client_lwt.Message.t -> Message.t

(** A subscription that runs a handler per delivery and settles each message
    with the broker according to the handler's verdict.

    [Retry] / [Dead_letter] verdicts are routed through delay-queue
    infrastructure (see {!Retry_infrastructure} and {!setup_retry}). *)

(** The settlement verdict, re-exported from {!Confirmation} so [Consumer.Ack]
    reads naturally. *)
type confirmation = Confirmation.t = Ack | Reject | Retry | Dead_letter

(** A message handler: given a delivery, decide how to settle it. *)
type handler = Confirmation.handler

(** A running subscription. *)
type t

(** Cancel the subscription. Safe to call more than once. *)
val stop : t -> unit Lwt.t

(** Declare the whole retry topology, idempotently: the retry exchange, the DLX,
    the main queue, the DLQ (bound to the DLX), and one delay queue per retry
    slot (each with its TTL and a dead-letter route back to the main queue). *)
val setup_retry :
  Connection.channel -> Retry_infrastructure.t -> (unit, Errors.t) result Lwt.t

(** Start a consumer on [queue]. Each delivery is passed to the handler, then
    settled with the broker per the returned {!confirmation}. *)
val start_consumer :
  Connection.channel ->
  string ->
  handler ->
  (t, Errors.t) result Lwt.t

(** Like {!start_consumer}, but with explicit {!Topology.consume_options}. *)
val start_consumer_with_options :
  Connection.channel ->
  string ->
  handler ->
  Topology.consume_options ->
  (t, Errors.t) result Lwt.t

(** Like {!start_consumer}, but processes up to [max_concurrent] deliveries at
    once (each as its own Lwt task; the broker's prefetch is set to match). *)
val start_consumer_concurrent :
  Connection.channel ->
  string ->
  handler ->
  int ->
  (t, Errors.t) result Lwt.t

(** Start a consumer backed by retry infrastructure. The topology is declared
    first (via {!setup_retry}), then the consumer subscribes to the
    infrastructure's main queue. *)
val start_consumer_with_retry :
  Connection.channel ->
  handler ->
  Retry_infrastructure.t ->
  (t, Errors.t) result Lwt.t

(** Start a consumer on [queue] that dispatches each delivery through a
    {!Router}. *)
val start_router :
  Connection.channel -> string -> Router.t -> (t, Errors.t) result Lwt.t

(** A router-backed consumer with retry infrastructure. *)
val start_router_with_retry :
  Connection.channel ->
  Router.t ->
  Retry_infrastructure.t ->
  (t, Errors.t) result Lwt.t

(**/**)

(* Internal: shared by {!Recoverable}. Not part of the stable public API. *)
val start_internal :
  Connection.channel ->
  queue:string ->
  handler:handler ->
  retry:Retry_infrastructure.t option ->
  concurrency:int ->
  consume:Topology.consume_options ->
  (t, Errors.t) result Lwt.t

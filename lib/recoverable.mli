(** A self-healing consumer that owns its own connection.

    If the connection drops, it reconnects with capped exponential backoff,
    re-opening the channel, re-declaring any retry topology, and re-subscribing.
    When [retry] infrastructure is configured the consumer subscribes to its
    main queue (and the [queue] argument is ignored). *)

type options = {
  prefetch : int option;
      (** Channel prefetch per (re)connect; if [None], uses [max_concurrent]
          when it is > 1. *)
  retry : Retry_infrastructure.t option;
      (** Retry infrastructure to (re)declare and back the consumer. *)
  on_connect : Connection.t -> unit Lwt.t;
      (** Hook run every time the consumer (re)establishes its connection. *)
  initial_backoff_ms : int;  (** Starting reconnection backoff (default 500). *)
  max_backoff_ms : int;  (** Ceiling on the backoff (default 30000). *)
  max_concurrent : int;  (** Prefetch-bounded concurrency (default 1). *)
  consume : Topology.consume_options;  (** Subscription options. *)
}

(** Build {!options}; unspecified fields take the defaults noted above. *)
val options :
  ?prefetch:int ->
  ?retry:Retry_infrastructure.t ->
  ?on_connect:(Connection.t -> unit Lwt.t) ->
  ?initial_backoff_ms:int ->
  ?max_backoff_ms:int ->
  ?max_concurrent:int ->
  ?consume:Topology.consume_options ->
  unit ->
  options

type t

(** Start a self-healing consumer. The result reports the {e first} connection
    attempt; subsequent drops are recovered transparently. *)
val start :
  Config.t ->
  string ->
  Confirmation.handler ->
  options ->
  (t, Errors.t) result Lwt.t

(** A recoverable consumer that dispatches deliveries through a {!Router}. *)
val start_router :
  Config.t -> string -> Router.t -> options -> (t, Errors.t) result Lwt.t

(** Stop the consumer and close its connection. *)
val stop : t -> unit Lwt.t

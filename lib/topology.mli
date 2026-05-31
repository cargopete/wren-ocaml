(** AMQP argument types, queue/exchange/consume options, and exchange kinds. *)

(** A typed value for an AMQP argument (the [x-*] settings on queues, exchanges
    and bindings - e.g. [x-message-ttl], [x-dead-letter-exchange]). *)
type arg = Int_arg of int | String_arg of string | Bool_arg of bool

(** The routing behaviour of an exchange. *)
type exchange_kind =
  | Direct  (** Exact routing-key match. *)
  | Fanout  (** Broadcast to every bound queue. *)
  | Topic  (** Wildcard routing-key match ([*] / [#]). *)
  | Headers  (** Match on message headers rather than routing key. *)

(** The on-the-wire exchange-type string, e.g. [Direct -> "direct"]. *)
val exchange_wire_name : exchange_kind -> string

(** Settings for declaring a queue. *)
type queue_options = {
  durable : bool;
  exclusive : bool;
  auto_delete : bool;
  arguments : (string * arg) list;
}

(** Durable, non-exclusive, non-auto-delete queue with no extra arguments - the
    sensible default. Override fields via the labelled optional arguments. *)
val queue_options :
  ?durable:bool ->
  ?exclusive:bool ->
  ?auto_delete:bool ->
  ?arguments:(string * arg) list ->
  unit ->
  queue_options

(** Settings for declaring an exchange. *)
type exchange_options = {
  durable : bool;
  auto_delete : bool;
  internal : bool;
  arguments : (string * arg) list;
}

(** Durable, non-auto-delete, non-internal exchange with no extra arguments. *)
val exchange_options :
  ?durable:bool ->
  ?auto_delete:bool ->
  ?internal:bool ->
  ?arguments:(string * arg) list ->
  unit ->
  exchange_options

(** How a consumer subscribes (the [basic.consume] knobs). *)
type consume_options = {
  auto_ack : bool;
      (** Let the broker consider messages acknowledged on delivery. With this
          on, the handler's {!Message.t} confirmation can't be honoured - the
          message is already gone - so settlement is skipped. *)
  exclusive : bool;  (** Request to be the only consumer on the queue. *)
  no_local : bool;
      (** Don't deliver messages published on this connection back to it. *)
  consumer_tag : string option;
      (** A specific consumer tag (the broker generates one if [None]). *)
  arguments : (string * arg) list;
      (** Extra [x-*] arguments for the subscription (e.g. consumer priority). *)
}

(** Default subscription: manual ack, non-exclusive, broker-assigned tag. *)
val consume_options :
  ?auto_ack:bool ->
  ?exclusive:bool ->
  ?no_local:bool ->
  ?consumer_tag:string ->
  ?arguments:(string * arg) list ->
  unit ->
  consume_options

(** {2 Fluent [with_*] builders for [consume_options], mirroring the Gleam API} *)

val with_auto_ack : consume_options -> consume_options
val with_exclusive : consume_options -> consume_options
val with_no_local : consume_options -> consume_options
val with_consumer_tag : consume_options -> string -> consume_options
val with_consume_arguments :
  consume_options -> (string * arg) list -> consume_options

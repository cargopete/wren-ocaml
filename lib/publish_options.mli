(** Options controlling how a message is published, plus batch/target types.

    Build with {!create} and refine with the [to_*] / [with_*] helpers. Mirrors
    the producer surface of the Rust [bunnyhop] crate: routing, headers,
    priority, per-message expiration, and the [mandatory] flag. *)

(** A standard AMQP message property. [Correlation_id] + [Reply_to] are the
    basis of request/reply. *)
type property =
  | Correlation_id of string
  | Reply_to of string
  | Message_id of string
  | Message_type of string
  | User_id of string
  | App_id of string
  | Content_encoding of string
  | Timestamp of int  (** A POSIX timestamp (seconds). *)

type t = {
  exchange : string;
      (** Exchange to publish to. [""] is the default exchange (route by queue
          name). *)
  routing_key : string;
      (** Routing key (or queue name when using the default exchange). *)
  headers : (string * string) list;  (** Arbitrary string headers. *)
  priority : int option;  (** Message priority (0-255 on a priority queue). *)
  expiration : int option;  (** Per-message TTL in milliseconds. *)
  mandatory : bool;
      (** Ask the broker to return the message if it can't be routed. *)
  content_type : string option;  (** MIME content type, e.g. ["application/json"]. *)
  persistent : bool;  (** Persist the message (delivery mode 2). *)
  properties : property list;  (** Standard AMQP message properties. *)
}

(** A blank set of publish options targeting the default exchange. *)
val create : unit -> t

(** {2 Builders} *)

val to_exchange : t -> string -> t
val route : t -> string -> t
val with_header : t -> string -> string -> t
val with_headers : t -> (string * string) list -> t
val with_priority : t -> int -> t
val with_expiration : t -> int -> t

(** Mark the publish as mandatory (broker returns unroutable messages). *)
val with_mandatory : t -> t

val with_content_type : t -> string -> t

(** Set the message [kind] header - sugar over [with_header t kind_header k]. *)
val with_kind : t -> string -> t

(** Mark the message persistent (delivery mode 2). *)
val with_persistence : t -> t

val with_property : t -> property -> t
val with_correlation_id : t -> string -> t
val with_reply_to : t -> string -> t
val with_message_id : t -> string -> t
val with_message_type : t -> string -> t
val with_timestamp : t -> int -> t

(** {2 Batch / multi-target publishing} *)

(** A publish destination: an exchange and routing key. *)
type target = { exchange : string; routing_key : string }

(** A target on the default exchange, routing by queue name. *)
val queue_target : string -> target

(** One message that couldn't be published, with the reason. *)
type batch_failure = {
  target : target;
  payload : string;
  error : Errors.t;
}

(** The outcome of a batch publish: how many succeeded, and which failed. *)
type batch_result = { published : int; failures : batch_failure list }

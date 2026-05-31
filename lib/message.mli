(** A delivered message handed to a consumer's handler.

    The [payload] is raw bytes (an OCaml [string]); use {!text} for the common
    UTF-8 case, or {!decode} with a codec to decode it. *)

type t = {
  payload : string;
  routing_key : string;
  headers : (string * string) list;
  correlation_id : string option;
      (** The AMQP [correlation_id] property, if set (pairs RPC replies). *)
  reply_to : string option;
      (** The AMQP [reply_to] property, if set (where to send an RPC reply). *)
  redelivered : bool;
      (** True if the broker has delivered this message before (e.g. after a
          requeue). *)
}

(** The header carrying a message's kind - the discriminator a router dispatches
    on. Matches the convention used by the Rust [bunnyhop] crate. *)
val kind_header : string

(** Read the [kind] header off a delivered message, if present. *)
val kind : t -> string option

(** The message payload as UTF-8 text, or [None] if it isn't valid UTF-8. *)
val text : t -> string option

(** Decode a delivered message's payload into a typed value with [codec]. *)
val decode : 'a Codec.t -> t -> ('a, Errors.t) result

(** Like {!decode} but raises {!Errors.Wren_error} on failure. *)
val decode_exn : 'a Codec.t -> t -> 'a

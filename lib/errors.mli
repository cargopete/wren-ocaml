(** Anything that can go wrong talking to the broker.

    The OCaml port carries errors as a plain variant in a [result], mirroring
    the Gleam [WrenError]. An exception form is also provided for the rare API
    that prefers to raise (e.g. {!Message.decode_exn}). *)

(** The error variant. *)
type t =
  | Connection_failed of string
      (** Failed to establish the underlying AMQP connection. *)
  | Channel_failed of string
      (** A channel-level operation (declare, publish, consume, ...) failed. *)
  | Encoding_failed of string
      (** A value could not be serialised before publishing. *)
  | Decoding_failed of string
      (** A payload could not be deserialised into the expected type. *)

(** [Wren_error e] is the exception form of an error, raised by the [*_exn]
    helpers and by code paths that bridge to exception-based callers. *)
exception Wren_error of t

(** The human-readable reason carried by an error. *)
val reason : t -> string

(** A short rendering of the error, e.g. ["connection_failed: ..."]. *)
val to_string : t -> string

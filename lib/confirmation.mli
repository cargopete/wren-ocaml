(** How a consumer wishes a delivered message to be settled with the broker.

    Shared by {!Router} and {!Consumer} (which re-exports the constructors, so
    [Consumer.Ack] and [Confirmation.Ack] are the same value). *)

type t =
  | Ack  (** Processed successfully - remove from the queue. *)
  | Reject
      (** Permanent failure - discard without redelivery or dead-lettering. *)
  | Retry  (** Transient failure - redeliver for another attempt. *)
  | Dead_letter
      (** Unprocessable - route to the dead-letter exchange, if configured. *)

(** A message handler: given a delivery, decide how to settle it. *)
type handler = Message.t -> t Lwt.t

(** Routes deliveries to handlers by their [kind] header.

    Build with {!create}, register typed handlers with {!handle} / {!handle_with},
    set a catch-all with {!fallback}, then run it under a consumer with
    {!Consumer.start_router}.

    Each typed handler is erased to a {!Confirmation.handler} by closing over its
    codec, so handlers for different message types live in one table. *)

type t

(** A new router whose default fallback rejects unrouted messages with a
    warning. *)
val create : unit -> t

(** Register a handler for messages of [kind]. The payload is decoded with
    [codec]; on a decode failure the message is rejected (and a warning logged),
    so the handler only ever sees well-formed values. *)
val handle :
  t -> string -> 'a Codec.t -> ('a -> Confirmation.t Lwt.t) -> t

(** Like {!handle}, but the handler also receives the raw {!Message.t} - its
    headers, routing key, and undecoded payload - as context. *)
val handle_with :
  t ->
  string ->
  'a Codec.t ->
  ('a -> Message.t -> Confirmation.t Lwt.t) ->
  t

(** Set the fallback handler invoked for messages whose [kind] has no registered
    handler (or that carry no [kind] header at all). *)
val fallback : t -> Confirmation.handler -> t

(** Dispatch a delivery through the router, returning the handler's verdict. *)
val dispatch : t -> Message.t -> Confirmation.t Lwt.t

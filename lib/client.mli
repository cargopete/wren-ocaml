(** A friendly front door bundling a connection and an open channel.

    Saves the connect-then-open-channel dance for the common case; reach for the
    accessors to use the wider {!Connection} API. *)

type t

(** Connect and open a channel in one step. *)
val start : Config.t -> (t, Errors.t) result Lwt.t

(** The client's open channel - pass it to {!Connection.publish},
    {!Connection.declare_queue}, the {!Consumer} functions, etc. *)
val channel : t -> Connection.channel

(** The client's underlying connection (e.g. for a second channel). *)
val connection : t -> Connection.t

(** The config the client was opened with. *)
val config : t -> Config.t

(** Close the client's channel and connection. *)
val close : t -> unit Lwt.t

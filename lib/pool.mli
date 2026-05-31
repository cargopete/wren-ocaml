(** A pool of open connections.

    {!channel} hands out channels round-robin across the pool, so heavy channel
    use is spread over several connections rather than crammed onto one. Build
    with {!start}, tear down with {!close}. *)

type t

(** A snapshot of a pool's activity. *)
type stats = { connections : int; channels_handed_out : int }

(** Open a pool of [size] connections (at least one). Rolls back any opened
    connections if one fails partway. *)
val start : Config.t -> int -> (t, Errors.t) result Lwt.t

(** How many connections the pool holds. *)
val size : t -> int

(** A snapshot: connection count and how many channels it has handed out over
    its lifetime. *)
val stats : t -> stats

(** Open a fresh channel on the pool's next connection (round-robin). Close it
    with {!Connection.close_channel} when you're done. *)
val channel : t -> (Connection.channel, Errors.t) result Lwt.t

(** Close every connection in the pool. *)
val close : t -> unit Lwt.t

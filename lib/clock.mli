(** A minimal wall clock for retry timestamps.

    Kept tiny and dependency-free (over [Unix]) so the rest of the library need
    not care how time is told. *)

(** The current UTC time as an RFC 3339 / ISO 8601 string, to the second, e.g.
    ["2026-05-31T22:04:00Z"]. Mirrors the Gleam port's [now_timestamp]. *)
val now_rfc3339 : unit -> string

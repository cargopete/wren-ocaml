(** Retry policy and per-message retry metadata.

    A {!policy} decides {e how long} to wait before the next attempt and {e how
    many} attempts to allow. {!metadata} is the running state carried on a
    message's headers as it bounces through the retry machinery.

    This module is pure: no broker, no clock. *)

(** How the delay before each retry is computed. *)
type strategy =
  | Exponential_backoff of { initial_ms : int; max_ms : int; multiplier : float }
      (** [initial_ms * multiplier ^ (attempt - 1)], capped at [max_ms]. *)
  | Fixed_interval of { interval_ms : int }
      (** The same [interval_ms] before every attempt. *)

(** A strategy plus a ceiling on how many attempts to make. *)
type policy = { strategy : strategy; max_attempts : int }

(** A common starting point: exponential backoff from 1s, doubling, capped at
    1 minute, over 5 attempts. *)
val default_policy : unit -> policy

(** The delay in milliseconds before [attempt] (1-based: attempt 1 is the first
    retry). Never negative; exponential delays are capped at [max_ms]. *)
val calculate_delay : policy -> int -> int

(** The full schedule of delays, one per allowed attempt. *)
val retry_intervals : policy -> int list

(** {2 Retry header names} *)

val retry_count_header : string  (** [x-retry-count] *)
val max_retries_header : string  (** [x-max-retries] *)
val first_death_header : string  (** [x-first-death] *)
val last_retry_header : string  (** [x-last-retry] *)
val original_error_header : string  (** [x-original-error] *)
val retry_reason_header : string  (** [x-retry-reason] *)
val original_routing_key_header : string  (** [x-original-routing-key] *)

(** The retry state carried on a message's headers. *)
type metadata = {
  attempt : int;
  max_attempts : int;
  first_death : string option;
  last_retry : string option;
  original_error : string option;
  reason : string option;
  original_routing_key : string option;
}

(** Fresh metadata for a message that has not yet failed. *)
val new_metadata : int -> metadata

(** Read retry metadata from message headers. When [x-max-retries] is absent,
    [default_max] is used. *)
val from_headers : (string * string) list -> int -> metadata

(** Serialise metadata back into headers. Always emits the count and maximum;
    optional fields are emitted only when present. *)
val to_headers : metadata -> (string * string) list

(** Record another failure: bump the attempt count and note the reason. *)
val record_failure : metadata -> string -> metadata

(** Has this message used up its allowed attempts? *)
val is_exhausted : metadata -> bool

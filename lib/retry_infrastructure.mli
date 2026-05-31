(** The broker topology that powers retries and dead-lettering for one main
    queue.

    When a handler asks to retry, the message is republished into a delay queue
    (a queue with a TTL and no consumer) that dead-letters back to the main
    queue when the TTL expires. Exhausted and dead-lettered messages go to the
    DLQ. Declare the topology with {!Connection.setup_retry}.

    Names are derived from the main queue: [<q>.retry], [<q>.dlx], [<q>.dlq]. *)

type t = {
  main_queue : string;
  retry_exchange : string;
  dlx_exchange : string;
  dlq : string;
  policy : Retry.policy;
}

(** Derive the retry topology for [main_queue] from a {!Retry.policy}. *)
val for_queue : string -> Retry.policy -> t

(** The routing key that lands a message in the DLQ ([dlq]). *)
val dlq_routing_key : string

(** The routing key that lands a message in the delay queue for [attempt].
    [Fixed_interval] always uses ["retry"]; [Exponential_backoff] uses
    ["attempt.<n>"] with [n] capped to [max_attempts]. *)
val routing_key_for_attempt : t -> int -> string

(** The delay queues to declare, as [(queue_name, routing_key, ttl_ms)]: one per
    attempt for exponential backoff (each with its own TTL), or a single queue
    for a fixed interval. *)
val slots : t -> (string * string * int) list

(** A map from message [kind] to its destination ([exchange], [routing_key]).

    {!apply} (and {!Connection.publish_for_kind}) consult it so producers don't
    repeat routing at every call site. Build with {!create} and {!route_kind} /
    {!route_kind_with_key}. *)

type t

(** An empty kind-routing table. *)
val create : unit -> t

(** Route [kind] to [exchange], using the kind itself as the routing key. *)
val route_kind : t -> kind:string -> exchange:string -> t

(** Route [kind] to [exchange] with an explicit [routing_key]. *)
val route_kind_with_key :
  t -> kind:string -> exchange:string -> routing_key:string -> t

(** The destination registered for [kind], if any. *)
val destination_for : t -> string -> Publish_options.target option

(** Apply the routing table for [kind] to [options]. An explicitly-set exchange
    on [options] wins; otherwise the table's mapping (if any) is used. A routing
    key still unset defaults to the kind (matching bunnyhop). *)
val apply : t -> string -> Publish_options.t -> Publish_options.t

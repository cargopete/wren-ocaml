module String_map = Map.Make (String)

(* kind -> (exchange, routing_key) *)
type t = (string * string) String_map.t

let create () = String_map.empty

let route_kind_with_key routing ~kind ~exchange ~routing_key =
  String_map.add kind (exchange, routing_key) routing

let route_kind routing ~kind ~exchange =
  route_kind_with_key routing ~kind ~exchange ~routing_key:kind

let destination_for routing kind =
  match String_map.find_opt kind routing with
  | Some (exchange, routing_key) ->
      Some Publish_options.{ exchange; routing_key }
  | None -> None

let apply routing kind (options : Publish_options.t) =
  (* An explicitly-set exchange takes precedence over the table. *)
  let options =
    if options.exchange <> "" then options
    else
      match String_map.find_opt kind routing with
      | Some (exchange, routing_key) -> { options with exchange; routing_key }
      | None -> options
  in
  (* A routing key still unset defaults to the kind (matching bunnyhop). *)
  if options.routing_key = "" then { options with routing_key = kind }
  else options

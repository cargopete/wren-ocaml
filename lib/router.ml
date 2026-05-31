module String_map = Map.Make (String)

type t = {
  handlers : Confirmation.handler String_map.t;
  fallback : Confirmation.handler;
}

let warn_and_reject message =
  let kind = match Message.kind message with Some k -> k | None -> "<none>" in
  Log.warning ("no handler for kind '" ^ kind ^ "', rejecting");
  Lwt.return Confirmation.Reject

let create () = { handlers = String_map.empty; fallback = warn_and_reject }

let handle_with router kind codec handler =
  let erased message =
    match Codec.decode codec message.Message.payload with
    | Ok value -> handler value message
    | Error error ->
        Log.warning
          ("dropping '" ^ kind ^ "' - payload failed to decode: "
         ^ Codec.error_reason error);
        Lwt.return Confirmation.Reject
  in
  { router with handlers = String_map.add kind erased router.handlers }

let handle router kind codec handler =
  handle_with router kind codec (fun value _message -> handler value)

let fallback router handler = { router with fallback = handler }

let dispatch router message =
  let handler =
    match Message.kind message with
    | Some kind -> (
        match String_map.find_opt kind router.handlers with
        | Some h -> h
        | None -> router.fallback)
    | None -> router.fallback
  in
  handler message

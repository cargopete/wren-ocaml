type t = {
  payload : string;
  routing_key : string;
  headers : (string * string) list;
  correlation_id : string option;
  reply_to : string option;
  redelivered : bool;
}

let kind_header = "kind"
let kind message = List.assoc_opt kind_header message.headers

let text message =
  if String.is_valid_utf_8 message.payload then Some message.payload else None

let decode codec message =
  match Codec.decode codec message.payload with
  | Ok value -> Ok value
  | Error error -> Error (Errors.Decoding_failed (Codec.error_reason error))

let decode_exn codec message =
  match decode codec message with
  | Ok value -> value
  | Error error -> raise (Errors.Wren_error error)

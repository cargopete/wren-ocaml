type error = Encode_error of string | Decode_error of string

type 'a t = {
  encode : 'a -> (string, error) result;
  decode : string -> ('a, error) result;
}

let encode codec value = codec.encode value
let decode codec payload = codec.decode payload

let error_reason = function Encode_error r | Decode_error r -> r

let json ~to_json ~of_json =
  {
    encode = (fun value -> Ok (Yojson.Safe.to_string (to_json value)));
    decode =
      (fun payload ->
        match Yojson.Safe.from_string payload with
        | json -> (
            match of_json json with
            | value -> Ok value
            | exception exn -> Error (Decode_error (Printexc.to_string exn)))
        | exception Yojson.Json_error msg -> Error (Decode_error msg)
        | exception exn -> Error (Decode_error (Printexc.to_string exn)));
  }

let string () =
  {
    encode = (fun value -> Ok value);
    decode =
      (fun payload ->
        if String.is_valid_utf_8 payload then Ok payload
        else Error (Decode_error "payload is not valid UTF-8"));
  }

let bytes () =
  { encode = (fun value -> Ok value); decode = (fun payload -> Ok payload) }

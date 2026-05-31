type t =
  | Connection_failed of string
  | Channel_failed of string
  | Encoding_failed of string
  | Decoding_failed of string

exception Wren_error of t

let reason = function
  | Connection_failed r | Channel_failed r | Encoding_failed r | Decoding_failed r
    -> r

let to_string = function
  | Connection_failed r -> "connection_failed: " ^ r
  | Channel_failed r -> "channel_failed: " ^ r
  | Encoding_failed r -> "encoding_failed: " ^ r
  | Decoding_failed r -> "decoding_failed: " ^ r

let () =
  Printexc.register_printer (function
    | Wren_error e -> Some ("Wren.Errors.Wren_error (" ^ to_string e ^ ")")
    | _ -> None)

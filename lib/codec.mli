(** Codecs turn typed values into wire payloads (bytes) and back again.

    A ['a t] is just a pair of functions, so you can supply any serialisation
    you like. {!json} builds one from a [Yojson] encoder/decoder; {!string} is
    the UTF-8 text codec, and {!bytes} is the identity codec for raw binary.

    This is wren's idiomatic stand-in for bunnyhop's [Codec] trait - explicit
    values rather than typeclass machinery. Payloads are plain [string]s, the
    natural byte container in OCaml (and what [amqp-client-lwt] uses). *)

(** Something went wrong encoding to, or decoding from, the wire. *)
type error =
  | Encode_error of string  (** A value could not be serialised. *)
  | Decode_error of string
      (** A payload could not be deserialised into the expected type. *)

(** A reversible mapping between a typed value ['a] and its byte payload. *)
type 'a t = {
  encode : 'a -> (string, error) result;
  decode : string -> ('a, error) result;
}

(** Encode a value with the given codec. *)
val encode : 'a t -> 'a -> (string, error) result

(** Decode a payload with the given codec. *)
val decode : 'a t -> string -> ('a, error) result

(** A JSON codec built from a [Yojson.Safe] encoder and decoder.

    [of_json] may raise; any exception (including [Yojson] parse errors) is
    caught and reported as a {!Decode_error}. *)
val json :
  to_json:('a -> Yojson.Safe.t) -> of_json:(Yojson.Safe.t -> 'a) -> 'a t

(** A UTF-8 text codec: values are [string]s, decoding fails on invalid UTF-8. *)
val string : unit -> string t

(** The identity codec: payloads are passed through as raw bytes. *)
val bytes : unit -> string t

(** The human-readable reason carried by a codec error. *)
val error_reason : error -> string

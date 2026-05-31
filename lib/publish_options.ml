type property =
  | Correlation_id of string
  | Reply_to of string
  | Message_id of string
  | Message_type of string
  | User_id of string
  | App_id of string
  | Content_encoding of string
  | Timestamp of int

type t = {
  exchange : string;
  routing_key : string;
  headers : (string * string) list;
  priority : int option;
  expiration : int option;
  mandatory : bool;
  content_type : string option;
  persistent : bool;
  properties : property list;
}

let create () =
  {
    exchange = "";
    routing_key = "";
    headers = [];
    priority = None;
    expiration = None;
    mandatory = false;
    content_type = None;
    persistent = false;
    properties = [];
  }

let to_exchange options exchange = { options with exchange }
let route options routing_key = { options with routing_key }

(* Prepend, matching the Gleam port: the most recently added header wins when an
   AMQP field table is built from the list. *)
let with_header options key value =
  { options with headers = (key, value) :: options.headers }

let with_headers options headers = { options with headers }
let with_priority options priority = { options with priority = Some priority }
let with_expiration options millis = { options with expiration = Some millis }
let with_mandatory options = { options with mandatory = true }

let with_content_type options content_type =
  { options with content_type = Some content_type }

let with_kind options kind = with_header options Message.kind_header kind
let with_persistence options = { options with persistent = true }

let with_property options property =
  { options with properties = property :: options.properties }

let with_correlation_id options id = with_property options (Correlation_id id)
let with_reply_to options queue = with_property options (Reply_to queue)
let with_message_id options id = with_property options (Message_id id)

let with_message_type options message_type =
  with_property options (Message_type message_type)

let with_timestamp options seconds = with_property options (Timestamp seconds)

type target = { exchange : string; routing_key : string }

let queue_target queue = { exchange = ""; routing_key = queue }

type batch_failure = { target : target; payload : string; error : Errors.t }
type batch_result = { published : int; failures : batch_failure list }

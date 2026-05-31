type arg = Int_arg of int | String_arg of string | Bool_arg of bool
type exchange_kind = Direct | Fanout | Topic | Headers

let exchange_wire_name = function
  | Direct -> "direct"
  | Fanout -> "fanout"
  | Topic -> "topic"
  | Headers -> "headers"

type queue_options = {
  durable : bool;
  exclusive : bool;
  auto_delete : bool;
  arguments : (string * arg) list;
}

let queue_options ?(durable = true) ?(exclusive = false) ?(auto_delete = false)
    ?(arguments = []) () =
  { durable; exclusive; auto_delete; arguments }

type exchange_options = {
  durable : bool;
  auto_delete : bool;
  internal : bool;
  arguments : (string * arg) list;
}

let exchange_options ?(durable = true) ?(auto_delete = false) ?(internal = false)
    ?(arguments = []) () =
  { durable; auto_delete; internal; arguments }

type consume_options = {
  auto_ack : bool;
  exclusive : bool;
  no_local : bool;
  consumer_tag : string option;
  arguments : (string * arg) list;
}

let consume_options ?(auto_ack = false) ?(exclusive = false) ?(no_local = false)
    ?consumer_tag ?(arguments = []) () =
  { auto_ack; exclusive; no_local; consumer_tag; arguments }

let with_auto_ack options = { options with auto_ack = true }
let with_exclusive options = { options with exclusive = true }
let with_no_local options = { options with no_local = true }
let with_consumer_tag options tag = { options with consumer_tag = Some tag }
let with_consume_arguments options arguments = { options with arguments }

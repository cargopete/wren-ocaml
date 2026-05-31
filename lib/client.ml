(* A ready-to-use connection paired with an open channel - saves the
   connect-then-open-channel dance for the common case. *)

open Lwt.Syntax

type t = {
  connection : Connection.t;
  channel : Connection.channel;
  config : Config.t;
}

let start config =
  let* conn = Connection.connect config in
  match conn with
  | Error e -> Lwt.return (Error e)
  | Ok connection -> (
      let* ch = Connection.open_channel connection in
      match ch with
      | Error e ->
          let* () = Connection.close_connection connection in
          Lwt.return (Error e)
      | Ok channel -> Lwt.return (Ok { connection; channel; config }))

let channel t = t.channel
let connection t = t.connection
let config t = t.config

let close t =
  let* () = Connection.close_channel t.channel in
  Connection.close_connection t.connection

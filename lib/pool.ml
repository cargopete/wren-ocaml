(* A pool of open connections. [channel] hands out channels round-robin across
   them, so heavy channel use is spread over several connections rather than
   crammed onto one.

   An Lwt mutex serialises access to the round-robin cursor, the OCaml analogue
   of the Gleam port's pool actor. *)

open Lwt.Syntax

type t = {
  connections : Connection.t array;
  mutable next : int;
  mutex : Lwt_mutex.t;
}

type stats = { connections : int; channels_handed_out : int }

let rec open_connections config remaining acc =
  if remaining = 0 then Lwt.return (Ok (List.rev acc))
  else
    let* conn = Connection.connect config in
    match conn with
    | Ok connection -> open_connections config (remaining - 1) (connection :: acc)
    | Error error ->
        (* Roll back any connections opened so far. *)
        let* () = Lwt_list.iter_p Connection.close_connection acc in
        Lwt.return (Error error)

let start config size =
  let size = max size 1 in
  let* conns = open_connections config size [] in
  match conns with
  | Error e -> Lwt.return (Error e)
  | Ok connections ->
      Lwt.return
        (Ok { connections = Array.of_list connections; next = 0; mutex = Lwt_mutex.create () })

let size (t : t) = Array.length t.connections

let stats (t : t) =
  { connections = Array.length t.connections; channels_handed_out = t.next }

let channel (t : t) =
  Lwt_mutex.with_lock t.mutex (fun () ->
      let index = t.next mod Array.length t.connections in
      t.next <- t.next + 1;
      Connection.open_channel t.connections.(index))

let close (t : t) =
  Lwt_list.iter_p Connection.close_connection (Array.to_list t.connections)

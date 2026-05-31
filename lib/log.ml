let src = Logs.Src.create "wren" ~doc:"wren AMQP client"

module L = (val Logs.src_log src : Logs.LOG)

let warning message =
  (* Mirror the Dart port: always surface on stderr, and also feed [Logs] so a
     host application can capture or reformat it. *)
  L.warn (fun m -> m "%s" message);
  Printf.eprintf "wren: %s\n%!" message

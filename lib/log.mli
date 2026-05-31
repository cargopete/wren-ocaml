(** Library-level logging.

    Warnings are emitted both through the [Logs] [wren] source (so an
    application can route them) and to [stderr] with a [wren:] prefix, matching
    the Dart port's behaviour out of the box. *)

(** The [Logs] source used for all wren warnings. *)
val src : Logs.src

(** Emit a warning. *)
val warning : string -> unit

type t = Ack | Reject | Retry | Dead_letter
type handler = Message.t -> t Lwt.t

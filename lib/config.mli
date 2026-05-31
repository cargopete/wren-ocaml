(** Connection settings. *)

(** TLS settings for a connection. [No_tls] is plaintext; [Tls] enables it. *)
type tls =
  | No_tls  (** Plaintext connection (the default). *)
  | Tls of {
      verify : bool;  (** Toggle peer verification. *)
      cacert_file : string option;  (** Optional CA-bundle PEM path. *)
      cert_file : string option;  (** Optional client-cert PEM path. *)
      key_file : string option;  (** Optional client-key PEM path. *)
    }

type t = {
  host : string;
  port : int;
  username : string;
  password : string;
  virtual_host : string;  (** The AMQP virtual host ([/] is the broker default). *)
  heartbeat_seconds : int;  (** Heartbeat interval in seconds ([0] disables). *)
  connection_timeout_ms : int;
      (** How long to wait for the TCP connection to establish, in ms. *)
  tls : tls;
}

(** Sensible localhost defaults ([guest]/[guest], vhost [/], plaintext). *)
val default : unit -> t

(** Build a config from the environment, reading the [RABBITMQ_*] variables.
    Anything unset (or an unparseable number) falls back to {!default}.

    Recognised: [RABBITMQ_HOST], [RABBITMQ_PORT], [RABBITMQ_USERNAME] (or
    [RABBITMQ_USER]), [RABBITMQ_PASSWORD] (or [RABBITMQ_PASS]), [RABBITMQ_VHOST],
    [RABBITMQ_HEARTBEAT], [RABBITMQ_CONNECTION_TIMEOUT]. *)
val from_env : unit -> t

(** Build a config from an arbitrary lookup function (env, a map, a config
    file...). Keys are the same [RABBITMQ_*] names as {!from_env}. *)
val from_lookup : (string -> string option) -> t

(** Check a config is well-formed (non-empty host, valid port). *)
val validate : t -> (unit, Errors.t) result

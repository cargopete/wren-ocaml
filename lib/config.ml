type tls =
  | No_tls
  | Tls of {
      verify : bool;
      cacert_file : string option;
      cert_file : string option;
      key_file : string option;
    }

type t = {
  host : string;
  port : int;
  username : string;
  password : string;
  virtual_host : string;
  heartbeat_seconds : int;
  connection_timeout_ms : int;
  tls : tls;
}

let default () =
  {
    host = "localhost";
    port = 5672;
    username = "guest";
    password = "guest";
    virtual_host = "/";
    heartbeat_seconds = 60;
    connection_timeout_ms = 10_000;
    tls = No_tls;
  }

let rec first_of lookup keys default =
  match keys with
  | [] -> default
  | key :: rest -> (
      match lookup key with Some value -> value | None -> first_of lookup rest default)

let int_or value default =
  match value with
  | Some raw -> ( match int_of_string_opt raw with Some n -> n | None -> default)
  | None -> default

let from_lookup lookup =
  let d = default () in
  {
    host = first_of lookup [ "RABBITMQ_HOST" ] d.host;
    port = int_or (lookup "RABBITMQ_PORT") d.port;
    username = first_of lookup [ "RABBITMQ_USERNAME"; "RABBITMQ_USER" ] d.username;
    password = first_of lookup [ "RABBITMQ_PASSWORD"; "RABBITMQ_PASS" ] d.password;
    virtual_host = first_of lookup [ "RABBITMQ_VHOST" ] d.virtual_host;
    heartbeat_seconds = int_or (lookup "RABBITMQ_HEARTBEAT") d.heartbeat_seconds;
    connection_timeout_ms =
      int_or (lookup "RABBITMQ_CONNECTION_TIMEOUT") d.connection_timeout_ms;
    tls = d.tls;
  }

let from_env () = from_lookup Sys.getenv_opt

let validate config =
  if config.host = "" then Error (Errors.Connection_failed "host must not be empty")
  else if config.port < 1 || config.port > 65_535 then
    Error (Errors.Connection_failed "port must be between 1 and 65535")
  else Ok ()

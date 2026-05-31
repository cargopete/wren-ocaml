open Wren

(* ------------------------------------------------------------------ *)
(* Codec                                                               *)
(* ------------------------------------------------------------------ *)

let map_codec : (string * Yojson.Safe.t) list Codec.t =
  Codec.json
    ~to_json:(fun fields -> `Assoc fields)
    ~of_json:(function `Assoc fields -> fields | _ -> failwith "expected object")

let codec_json_roundtrip () =
  let value = [ ("id", `String "A-1"); ("qty", `Int 3) ] in
  match Codec.encode map_codec value with
  | Error _ -> Alcotest.fail "encode failed"
  | Ok payload -> (
      match Codec.decode map_codec payload with
      | Ok decoded ->
          Alcotest.(check string) "id" "A-1"
            (match List.assoc "id" decoded with `String s -> s | _ -> "?");
          Alcotest.(check int) "qty" 3
            (match List.assoc "qty" decoded with `Int n -> n | _ -> -1)
      | Error _ -> Alcotest.fail "decode failed")

let codec_json_bad_json () =
  match Codec.decode map_codec "not json" with
  | Error (Codec.Decode_error _) -> ()
  | _ -> Alcotest.fail "expected Decode_error on invalid JSON"

let codec_json_bad_utf8 () =
  match Codec.decode map_codec "\xff\xfe" with
  | Error (Codec.Decode_error _) -> ()
  | _ -> Alcotest.fail "expected Decode_error on invalid UTF-8"

let codec_string_roundtrip () =
  let codec = Codec.string () in
  match Codec.encode codec "h\xc3\xa9llo" with
  | Ok payload -> (
      match Codec.decode codec payload with
      | Ok s -> Alcotest.(check string) "roundtrip" "h\xc3\xa9llo" s
      | Error _ -> Alcotest.fail "decode failed")
  | Error _ -> Alcotest.fail "encode failed"

let codec_string_bad_utf8 () =
  match Codec.decode (Codec.string ()) "\xff" with
  | Error (Codec.Decode_error _) -> ()
  | _ -> Alcotest.fail "expected Decode_error on invalid UTF-8"

let codec_bytes_passthrough () =
  let codec = Codec.bytes () in
  let raw = "\x01\x02\x03" in
  match Codec.encode codec raw with
  | Ok enc -> (
      match Codec.decode codec enc with
      | Ok dec -> Alcotest.(check string) "passthrough" raw dec
      | Error _ -> Alcotest.fail "decode failed")
  | Error _ -> Alcotest.fail "encode failed"

(* ------------------------------------------------------------------ *)
(* Config                                                              *)
(* ------------------------------------------------------------------ *)

let config_from_lookup () =
  let env =
    [
      ("RABBITMQ_HOST", "broker.internal");
      ("RABBITMQ_PORT", "5673");
      ("RABBITMQ_USER", "app");
      ("RABBITMQ_PASSWORD", "s3cret");
      ("RABBITMQ_VHOST", "/prod");
      ("RABBITMQ_HEARTBEAT", "30");
    ]
  in
  let c = Config.from_lookup (fun k -> List.assoc_opt k env) in
  Alcotest.(check string) "host" "broker.internal" c.host;
  Alcotest.(check int) "port" 5673 c.port;
  Alcotest.(check string) "user" "app" c.username;
  Alcotest.(check string) "pass" "s3cret" c.password;
  Alcotest.(check string) "vhost" "/prod" c.virtual_host;
  Alcotest.(check int) "heartbeat" 30 c.heartbeat_seconds

let config_fallbacks () =
  let c =
    Config.from_lookup (fun k ->
        if k = "RABBITMQ_PORT" then Some "not-a-number" else None)
  in
  let d = Config.default () in
  Alcotest.(check string) "host" d.host c.host;
  Alcotest.(check int) "port" d.port c.port;
  Alcotest.(check string) "user" d.username c.username

let config_validate () =
  (match Config.validate (Config.default ()) with
  | Ok () -> ()
  | Error _ -> Alcotest.fail "default should validate");
  (match Config.validate { (Config.default ()) with host = "" } with
  | Error (Errors.Connection_failed _) -> ()
  | _ -> Alcotest.fail "empty host should fail");
  match Config.validate { (Config.default ()) with port = 70000 } with
  | Error (Errors.Connection_failed _) -> ()
  | _ -> Alcotest.fail "out-of-range port should fail"

(* ------------------------------------------------------------------ *)
(* Message                                                            *)
(* ------------------------------------------------------------------ *)

let message_kind_and_text () =
  let m =
    {
      Message.payload = "hi";
      routing_key = "";
      headers = [ (Message.kind_header, "greeting") ];
      correlation_id = None;
      reply_to = None;
      redelivered = false;
    }
  in
  Alcotest.(check (option string)) "kind" (Some "greeting") (Message.kind m);
  Alcotest.(check (option string)) "text" (Some "hi") (Message.text m)

let message_decode () =
  let m =
    {
      Message.payload = "{\"id\":\"A-1\"}";
      routing_key = "";
      headers = [];
      correlation_id = None;
      reply_to = None;
      redelivered = false;
    }
  in
  match Message.decode map_codec m with
  | Ok fields ->
      Alcotest.(check string) "id" "A-1"
        (match List.assoc "id" fields with `String s -> s | _ -> "?")
  | Error _ -> Alcotest.fail "decode failed"

let message_decode_wraps_error () =
  let m =
    {
      Message.payload = "not-json";
      routing_key = "";
      headers = [];
      correlation_id = None;
      reply_to = None;
      redelivered = false;
    }
  in
  match Message.decode map_codec m with
  | Error (Errors.Decoding_failed _) -> ()
  | _ -> Alcotest.fail "expected Decoding_failed"

(* ------------------------------------------------------------------ *)
(* Publish options                                                    *)
(* ------------------------------------------------------------------ *)

let publish_defaults () =
  let o = Publish_options.create () in
  Alcotest.(check string) "exchange" "" o.exchange;
  Alcotest.(check string) "routing_key" "" o.routing_key;
  Alcotest.(check bool) "headers empty" true (o.headers = []);
  Alcotest.(check bool) "not persistent" false o.persistent

let publish_chain () =
  let open Publish_options in
  let base = create () in
  let o =
    base |> fun o -> to_exchange o "events" |> fun o -> route o "orders"
    |> fun o -> with_header o "x" "1" |> fun o -> with_priority o 5
    |> fun o -> with_expiration o 1000 |> with_mandatory |> with_persistence
    |> fun o -> with_content_type o "application/json"
  in
  Alcotest.(check string) "exchange" "events" o.exchange;
  Alcotest.(check string) "routing_key" "orders" o.routing_key;
  Alcotest.(check (option string)) "header x" (Some "1")
    (List.assoc_opt "x" o.headers);
  Alcotest.(check (option int)) "priority" (Some 5) o.priority;
  Alcotest.(check (option int)) "expiration" (Some 1000) o.expiration;
  Alcotest.(check bool) "mandatory" true o.mandatory;
  Alcotest.(check bool) "persistent" true o.persistent;
  Alcotest.(check (option string)) "content_type" (Some "application/json")
    o.content_type;
  (* original untouched *)
  Alcotest.(check string) "base exchange" "" base.exchange;
  Alcotest.(check bool) "base headers empty" true (base.headers = [])

let publish_with_kind () =
  let o = Publish_options.with_kind (Publish_options.create ()) "order.created" in
  Alcotest.(check (option string))
    "kind header" (Some "order.created")
    (List.assoc_opt Message.kind_header o.headers)

(* ------------------------------------------------------------------ *)
(* Retry policy + metadata                                            *)
(* ------------------------------------------------------------------ *)

let retry_exponential () =
  let policy =
    {
      Retry.strategy =
        Exponential_backoff { initial_ms = 1000; max_ms = 5000; multiplier = 2.0 };
      max_attempts = 5;
    }
  in
  Alcotest.(check int) "1" 1000 (Retry.calculate_delay policy 1);
  Alcotest.(check int) "2" 2000 (Retry.calculate_delay policy 2);
  Alcotest.(check int) "3" 4000 (Retry.calculate_delay policy 3);
  Alcotest.(check int) "4 capped" 5000 (Retry.calculate_delay policy 4);
  Alcotest.(check int) "5 capped" 5000 (Retry.calculate_delay policy 5)

let retry_attempt_floor () =
  let policy =
    {
      Retry.strategy =
        Exponential_backoff { initial_ms = 100; max_ms = 9999; multiplier = 2.0 };
      max_attempts = 3;
    }
  in
  Alcotest.(check int) "attempt 0 treated as 1" 100
    (Retry.calculate_delay policy 0)

let retry_fixed () =
  let policy = { Retry.strategy = Fixed_interval { interval_ms = 250 }; max_attempts = 3 } in
  Alcotest.(check int) "constant 1" 250 (Retry.calculate_delay policy 1);
  Alcotest.(check int) "constant 9" 250 (Retry.calculate_delay policy 9);
  let neg = { Retry.strategy = Fixed_interval { interval_ms = -5 }; max_attempts = 1 } in
  Alcotest.(check int) "never negative" 0 (Retry.calculate_delay neg 1)

let retry_intervals_count () =
  Alcotest.(check int) "five" 5
    (List.length (Retry.retry_intervals (Retry.default_policy ())))

let retry_metadata_roundtrip () =
  let m =
    {
      (Retry.new_metadata 5) with
      attempt = 2;
      first_death = Some "2026-01-01T00:00:00Z";
      reason = Some "boom";
    }
  in
  let headers = Retry.to_headers m in
  Alcotest.(check (option string)) "count" (Some "2")
    (List.assoc_opt Retry.retry_count_header headers);
  Alcotest.(check (option string)) "max" (Some "5")
    (List.assoc_opt Retry.max_retries_header headers);
  Alcotest.(check (option string)) "first death" (Some "2026-01-01T00:00:00Z")
    (List.assoc_opt Retry.first_death_header headers);
  Alcotest.(check (option string)) "reason" (Some "boom")
    (List.assoc_opt Retry.retry_reason_header headers);
  let parsed = Retry.from_headers headers 99 in
  Alcotest.(check int) "parsed attempt" 2 parsed.attempt;
  Alcotest.(check int) "parsed max" 5 parsed.max_attempts;
  Alcotest.(check (option string)) "parsed reason" (Some "boom") parsed.reason

let retry_metadata_default_max () =
  let parsed = Retry.from_headers [] 7 in
  Alcotest.(check int) "attempt" 0 parsed.attempt;
  Alcotest.(check int) "max" 7 parsed.max_attempts

let retry_record_failure () =
  let m = Retry.record_failure (Retry.new_metadata 3) "nope" in
  Alcotest.(check int) "attempt" 1 m.attempt;
  Alcotest.(check (option string)) "reason" (Some "nope") m.reason

let retry_exhausted () =
  Alcotest.(check bool) "3/3 exhausted" true
    (Retry.is_exhausted { (Retry.new_metadata 3) with attempt = 3 });
  Alcotest.(check bool) "2/3 not" false
    (Retry.is_exhausted { (Retry.new_metadata 3) with attempt = 2 })

(* ------------------------------------------------------------------ *)
(* Retry infrastructure                                               *)
(* ------------------------------------------------------------------ *)

let infra_names () =
  let i = Retry_infrastructure.for_queue "orders" (Retry.default_policy ()) in
  Alcotest.(check string) "main" "orders" i.main_queue;
  Alcotest.(check string) "retry" "orders.retry" i.retry_exchange;
  Alcotest.(check string) "dlx" "orders.dlx" i.dlx_exchange;
  Alcotest.(check string) "dlq" "orders.dlq" i.dlq

let infra_routing_key_exponential () =
  let i =
    Retry_infrastructure.for_queue "orders"
      {
        Retry.strategy =
          Exponential_backoff { initial_ms = 100; max_ms = 1000; multiplier = 2.0 };
        max_attempts = 3;
      }
  in
  Alcotest.(check string) "1" "attempt.1" (Retry_infrastructure.routing_key_for_attempt i 1);
  Alcotest.(check string) "2" "attempt.2" (Retry_infrastructure.routing_key_for_attempt i 2);
  Alcotest.(check string) "capped" "attempt.3"
    (Retry_infrastructure.routing_key_for_attempt i 9)

let infra_routing_key_fixed () =
  let i =
    Retry_infrastructure.for_queue "orders"
      { Retry.strategy = Fixed_interval { interval_ms = 500 }; max_attempts = 3 }
  in
  Alcotest.(check string) "1" "retry" (Retry_infrastructure.routing_key_for_attempt i 1);
  Alcotest.(check string) "2" "retry" (Retry_infrastructure.routing_key_for_attempt i 2)

let infra_slots () =
  let i =
    Retry_infrastructure.for_queue "orders"
      {
        Retry.strategy =
          Exponential_backoff { initial_ms = 100; max_ms = 1000; multiplier = 2.0 };
        max_attempts = 3;
      }
  in
  let slots = Retry_infrastructure.slots i in
  Alcotest.(check int) "three slots" 3 (List.length slots);
  let q, rk, ttl = List.hd slots in
  Alcotest.(check string) "slot queue" "orders.retry.1" q;
  Alcotest.(check string) "slot key" "attempt.1" rk;
  Alcotest.(check int) "slot ttl" 100 ttl

(* ------------------------------------------------------------------ *)
(* Kind routing                                                       *)
(* ------------------------------------------------------------------ *)

let kind_uses_table () =
  let r =
    Kind_routing.route_kind (Kind_routing.create ()) ~kind:"order.created"
      ~exchange:"orders"
  in
  let o = Kind_routing.apply r "order.created" (Publish_options.create ()) in
  Alcotest.(check string) "exchange" "orders" o.exchange;
  Alcotest.(check string) "routing_key" "order.created" o.routing_key

let kind_explicit_exchange_wins () =
  let r =
    Kind_routing.route_kind (Kind_routing.create ()) ~kind:"order.created"
      ~exchange:"orders"
  in
  let opts = Publish_options.to_exchange (Publish_options.create ()) "override" in
  let o = Kind_routing.apply r "order.created" opts in
  Alcotest.(check string) "override wins" "override" o.exchange

let kind_unset_defaults_to_kind () =
  let o =
    Kind_routing.apply (Kind_routing.create ()) "order.created"
      (Publish_options.create ())
  in
  Alcotest.(check string) "exchange" "" o.exchange;
  Alcotest.(check string) "routing_key" "order.created" o.routing_key

let kind_explicit_key () =
  let r =
    Kind_routing.route_kind_with_key (Kind_routing.create ()) ~kind:"order.created"
      ~exchange:"orders" ~routing_key:"new"
  in
  let o = Kind_routing.apply r "order.created" (Publish_options.create ()) in
  Alcotest.(check string) "explicit key" "new" o.routing_key

(* ------------------------------------------------------------------ *)
(* Router                                                             *)
(* ------------------------------------------------------------------ *)

let int_codec : int Codec.t =
  Codec.json
    ~to_json:(fun i -> `Int i)
    ~of_json:(function `Int i -> i | _ -> failwith "expected int")

let msg ?kind payload =
  {
    Message.payload;
    routing_key = "";
    headers = (match kind with Some k -> [ (Message.kind_header, k) ] | None -> []);
    correlation_id = None;
    reply_to = None;
    redelivered = false;
  }

let router_dispatches () =
  let seen = ref 0 in
  let r =
    Router.handle (Router.create ()) "count" int_codec (fun v ->
        seen := v;
        Lwt.return Confirmation.Ack)
  in
  let result = Lwt_main.run (Router.dispatch r (msg ~kind:"count" "41")) in
  Alcotest.(check bool) "ack" true (result = Confirmation.Ack);
  Alcotest.(check int) "seen" 41 !seen

let router_fallback_no_handler () =
  let r =
    Router.fallback
      (Router.handle (Router.create ()) "count" int_codec (fun _ ->
           Lwt.return Confirmation.Ack))
      (fun _ -> Lwt.return Confirmation.Dead_letter)
  in
  let result = Lwt_main.run (Router.dispatch r (msg ~kind:"other" "1")) in
  Alcotest.(check bool) "dead_letter" true (result = Confirmation.Dead_letter)

let router_fallback_no_kind () =
  let r = Router.fallback (Router.create ()) (fun _ -> Lwt.return Confirmation.Reject) in
  let result = Lwt_main.run (Router.dispatch r (msg "1")) in
  Alcotest.(check bool) "reject" true (result = Confirmation.Reject)

let router_rejects_bad_payload () =
  let ran = ref false in
  let r =
    Router.handle (Router.create ()) "count" int_codec (fun _ ->
        ran := true;
        Lwt.return Confirmation.Ack)
  in
  let result = Lwt_main.run (Router.dispatch r (msg ~kind:"count" "not-an-int")) in
  Alcotest.(check bool) "reject" true (result = Confirmation.Reject);
  Alcotest.(check bool) "handler did not run" false !ran

let router_handle_with_message () =
  let seen = ref "" in
  let r =
    Router.handle_with (Router.create ()) "count" int_codec (fun _ m ->
        seen := m.Message.routing_key;
        Lwt.return Confirmation.Ack)
  in
  let m = { (msg ~kind:"count" "1") with Message.routing_key = "rk" } in
  let _ = Lwt_main.run (Router.dispatch r m) in
  Alcotest.(check string) "routing key" "rk" !seen

(* ------------------------------------------------------------------ *)

let () =
  Alcotest.run "wren"
    [
      ( "codec",
        [
          Alcotest.test_case "json roundtrip" `Quick codec_json_roundtrip;
          Alcotest.test_case "json bad json" `Quick codec_json_bad_json;
          Alcotest.test_case "json bad utf8" `Quick codec_json_bad_utf8;
          Alcotest.test_case "string roundtrip" `Quick codec_string_roundtrip;
          Alcotest.test_case "string bad utf8" `Quick codec_string_bad_utf8;
          Alcotest.test_case "bytes passthrough" `Quick codec_bytes_passthrough;
        ] );
      ( "config",
        [
          Alcotest.test_case "from_lookup" `Quick config_from_lookup;
          Alcotest.test_case "fallbacks" `Quick config_fallbacks;
          Alcotest.test_case "validate" `Quick config_validate;
        ] );
      ( "message",
        [
          Alcotest.test_case "kind and text" `Quick message_kind_and_text;
          Alcotest.test_case "decode" `Quick message_decode;
          Alcotest.test_case "decode wraps error" `Quick message_decode_wraps_error;
        ] );
      ( "publish_options",
        [
          Alcotest.test_case "defaults" `Quick publish_defaults;
          Alcotest.test_case "chain immutable" `Quick publish_chain;
          Alcotest.test_case "with_kind" `Quick publish_with_kind;
        ] );
      ( "retry",
        [
          Alcotest.test_case "exponential" `Quick retry_exponential;
          Alcotest.test_case "attempt floor" `Quick retry_attempt_floor;
          Alcotest.test_case "fixed" `Quick retry_fixed;
          Alcotest.test_case "intervals count" `Quick retry_intervals_count;
          Alcotest.test_case "metadata roundtrip" `Quick retry_metadata_roundtrip;
          Alcotest.test_case "metadata default max" `Quick retry_metadata_default_max;
          Alcotest.test_case "record failure" `Quick retry_record_failure;
          Alcotest.test_case "exhausted" `Quick retry_exhausted;
        ] );
      ( "retry_infrastructure",
        [
          Alcotest.test_case "names" `Quick infra_names;
          Alcotest.test_case "routing key exponential" `Quick infra_routing_key_exponential;
          Alcotest.test_case "routing key fixed" `Quick infra_routing_key_fixed;
          Alcotest.test_case "slots" `Quick infra_slots;
        ] );
      ( "kind_routing",
        [
          Alcotest.test_case "uses table" `Quick kind_uses_table;
          Alcotest.test_case "explicit exchange wins" `Quick kind_explicit_exchange_wins;
          Alcotest.test_case "unset defaults to kind" `Quick kind_unset_defaults_to_kind;
          Alcotest.test_case "explicit key" `Quick kind_explicit_key;
        ] );
      ( "router",
        [
          Alcotest.test_case "dispatches" `Quick router_dispatches;
          Alcotest.test_case "fallback no handler" `Quick router_fallback_no_handler;
          Alcotest.test_case "fallback no kind" `Quick router_fallback_no_kind;
          Alcotest.test_case "rejects bad payload" `Quick router_rejects_bad_payload;
          Alcotest.test_case "handle_with message" `Quick router_handle_with_message;
        ] );
    ]

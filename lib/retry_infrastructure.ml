type t = {
  main_queue : string;
  retry_exchange : string;
  dlx_exchange : string;
  dlq : string;
  policy : Retry.policy;
}

let for_queue main_queue policy =
  {
    main_queue;
    retry_exchange = main_queue ^ ".retry";
    dlx_exchange = main_queue ^ ".dlx";
    dlq = main_queue ^ ".dlq";
    policy;
  }

let dlq_routing_key = "dlq"

let routing_key_for_attempt infra attempt =
  match infra.policy.strategy with
  | Retry.Fixed_interval _ -> "retry"
  | Retry.Exponential_backoff _ ->
      let capped = min (max attempt 1) infra.policy.max_attempts in
      "attempt." ^ string_of_int capped

let slots infra =
  match infra.policy.strategy with
  | Retry.Fixed_interval { interval_ms } ->
      [ (infra.main_queue ^ ".retry", "retry", max interval_ms 0) ]
  | Retry.Exponential_backoff _ ->
      List.init infra.policy.max_attempts (fun i ->
          let attempt = i + 1 in
          ( infra.main_queue ^ ".retry." ^ string_of_int attempt,
            "attempt." ^ string_of_int attempt,
            Retry.calculate_delay infra.policy attempt ))

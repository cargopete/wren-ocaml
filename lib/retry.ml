type strategy =
  | Exponential_backoff of { initial_ms : int; max_ms : int; multiplier : float }
  | Fixed_interval of { interval_ms : int }

type policy = { strategy : strategy; max_attempts : int }

let default_policy () =
  {
    strategy = Exponential_backoff { initial_ms = 1000; max_ms = 60_000; multiplier = 2.0 };
    max_attempts = 5;
  }

let calculate_delay policy attempt =
  let attempt = max attempt 1 in
  match policy.strategy with
  | Fixed_interval { interval_ms } -> max interval_ms 0
  | Exponential_backoff { initial_ms; max_ms; multiplier } ->
      let factor = multiplier ** float_of_int (attempt - 1) in
      let delay = int_of_float (Float.round (float_of_int initial_ms *. factor)) in
      max (min delay max_ms) 0

let retry_intervals policy =
  List.init policy.max_attempts (fun i -> calculate_delay policy (i + 1))

let retry_count_header = "x-retry-count"
let max_retries_header = "x-max-retries"
let first_death_header = "x-first-death"
let last_retry_header = "x-last-retry"
let original_error_header = "x-original-error"
let retry_reason_header = "x-retry-reason"
let original_routing_key_header = "x-original-routing-key"

type metadata = {
  attempt : int;
  max_attempts : int;
  first_death : string option;
  last_retry : string option;
  original_error : string option;
  reason : string option;
  original_routing_key : string option;
}

let new_metadata max_attempts =
  {
    attempt = 0;
    max_attempts;
    first_death = None;
    last_retry = None;
    original_error = None;
    reason = None;
    original_routing_key = None;
  }

let header_int headers key default =
  match List.assoc_opt key headers with
  | Some value -> ( match int_of_string_opt value with Some n -> n | None -> default)
  | None -> default

let header_opt headers key = List.assoc_opt key headers

let from_headers headers default_max =
  {
    attempt = header_int headers retry_count_header 0;
    max_attempts = header_int headers max_retries_header default_max;
    first_death = header_opt headers first_death_header;
    last_retry = header_opt headers last_retry_header;
    original_error = header_opt headers original_error_header;
    reason = header_opt headers retry_reason_header;
    original_routing_key = header_opt headers original_routing_key_header;
  }

let push_opt headers key = function
  | Some value -> (key, value) :: headers
  | None -> headers

let to_headers metadata =
  [
    (retry_count_header, string_of_int metadata.attempt);
    (max_retries_header, string_of_int metadata.max_attempts);
  ]
  |> fun h -> push_opt h first_death_header metadata.first_death
  |> fun h -> push_opt h last_retry_header metadata.last_retry
  |> fun h -> push_opt h original_error_header metadata.original_error
  |> fun h -> push_opt h retry_reason_header metadata.reason
  |> fun h -> push_opt h original_routing_key_header metadata.original_routing_key

let record_failure metadata reason =
  { metadata with attempt = metadata.attempt + 1; reason = Some reason }

let is_exhausted metadata = metadata.attempt >= metadata.max_attempts

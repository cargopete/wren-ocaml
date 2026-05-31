(lang dune 3.0)

(name wren)

(generate_opam_files true)

(source
 (github cargopete/wren-ocaml))

(license MIT)

(authors "cargopete")

(maintainers "cargopete")

(package
 (name wren)
 (synopsis "An ergonomic RabbitMQ / AMQP messaging library for OCaml")
 (description
  "Typed connections and channels, a router-style consumer, retries and dead-lettering, and self-healing reconnection - a friendly layer over amqp-client-lwt. The OCaml sibling of the Gleam and Dart `wren` libraries.")
 (depends
  (ocaml
   (>= 5.0))
  (lwt
   (>= 5.6.0))
  lwt_ppx
  amqp-client-lwt
  yojson
  logs
  (alcotest :with-test)))

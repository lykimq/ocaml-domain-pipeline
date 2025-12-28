(lang dune 2.9)

(name domain_pipeline)

(generate_opam_files true)

(source
 (github lykimq/ocaml-domain-pipeline))

(authors "Ly Kim Quyen <lykimq@gmail.com>")

(maintainers "Ly Kim Quyen <lykimq@gmail.com>")

(license MIT)

(documentation https://github.com/lykimq/ocaml-domain-pipeline)

(package
 (name domain_pipeline)
 (synopsis "Domain-parallel pipeline library for OCaml 5")
 (description
  "A high-level pipeline abstraction for OCaml 5's domain-based parallelism, providing bounded queues, backpressure, cancellation, and error propagation.")
 (depends
  (ocaml (>= 5.0))
  dune
  threads))


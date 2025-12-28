# ocaml-domain-pipeline

A domain-parallel pipeline library for OCaml 5. Build multi-stage data
processing pipelines where each stage runs in parallel across configurable
domains, connected by bounded queues with automatic backpressure. Ideal for
I/O-bound workloads like web crawling, log processing, and data transformation.

## Status

This is a **working implementation** suitable for production use. The core API
is stable, and the architecture has been validated through testing. See
[PROTOTYPE.md](docs/PROTOTYPE.md) for implementation details, performance
characteristics, and the production roadmap.

## When to Use This Library

**Good fit:**
- Multi-stage data pipelines (fetch -> parse -> extract -> store)
- I/O-bound workloads where parallelism provides real speedup
- Scenarios where you need bounded queues to prevent memory growth
- Projects requiring structured error handling with stage context

**Not ideal for:**
- CPU-bound workloads requiring extreme throughput (see performance notes in
  [PROTOTYPE.md](docs/PROTOTYPE.md))
- Simple parallel tasks better served by `Domainslib.Parallel_for`
- Real-time systems with hard latency requirements

## Quick Start

```ocaml
open Domain_pipeline

(* Define a pipeline: source -> stages -> sink *)
let pipeline =
  Pipeline.source (fun () -> List.init 20 (fun i -> i + 1))
  |> Pipeline.stage ~name:"double" ~parallelism:4 (fun n -> n * 2)
  |> Pipeline.stage ~name:"square" ~parallelism:2 (fun n -> n * n)

(* Run it *)
let results = Pipeline.run_exn pipeline
```

For a more complete example, see `examples/simple_pipeline.ml`.

## Core Concepts

**Stages**: Each stage processes items in parallel. You specify the number of
worker domains per stage via `~parallelism`. Stages are connected by bounded
queues.

**Bounded Queues**: Queues between stages have a fixed capacity (default 100
items). When full, producers block—this backpressure prevents runaway memory use
when stages have different speeds.

**Backpressure**: If stage N is slow and fills its output queue, stage N-1 will
block on push until space frees up. This natural flow control keeps the pipeline
balanced.

**Error Handling**: Errors are captured with the stage name and exception
details. The pipeline doesn't crash; it collects errors and reports them via
`Pipeline.Error` or via the `~on_error` callback.

## API Overview

```ocaml
(* Create a pipeline source *)
val source : (unit -> 'a list) -> ('a, 'a) t

(* Add a processing stage *)
val stage : name:string -> ?parallelism:int -> ('a -> 'b) -> 
  ('input, 'a) t -> ('input, 'b) t

(* Add a sink (final consumer, no output) *)
val sink : ('a -> unit) -> ('input, 'a) t -> ('input, unit) t

(* Run the pipeline *)
val run : ?queue_capacity:int -> ?on_error:(error -> unit) ->
  ('input, 'output) t -> 'output result

(* Run and raise on error *)
val run_exn : ?queue_capacity:int -> ('input, 'output) t -> 'output list
```

See `lib/pipeline.mli` for full documentation with examples.

## Building

Requires OCaml 5.x:

```bash
opam switch create . 5.2.1
eval $(opam env)
dune build
dune exec examples/simple_pipeline.exe
```

## Example: Web Crawler Pattern

```ocaml
open Domain_pipeline

let urls = ["http://example.com"; "http://example.org"]

let pipeline =
  Pipeline.source (fun () -> urls)
  |> Pipeline.stage ~name:"fetch" ~parallelism:4 fetch_page
  |> Pipeline.stage ~name:"parse" ~parallelism:2 parse_html
  |> Pipeline.stage ~name:"extract" ~parallelism:2 extract_links
  |> Pipeline.sink (fun link -> save_to_db link)

let () =
  match Pipeline.run ~on_error:(fun e -> 
    Printf.eprintf "Error: %s\n" (Pipeline.error_to_string e)
  ) pipeline with
  | Pipeline.Ok _ -> Printf.printf "Crawl complete\n"
  | Pipeline.Error errors -> Printf.printf "Failed with %d errors\n" (List.length errors)
  | Pipeline.Cancelled -> Printf.printf "Crawl cancelled\n"
```

## Performance

This library prioritizes correctness and ease of use. For I/O-bound workloads,
expect near-linear scaling with parallelism (up to 8 workers per stage). For
CPU-bound work, current performance is functional but not optimal; see
[PROTOTYPE.md](docs/PROTOTYPE.md) for technical details and future optimization
plans.

## Development

See [PROTOTYPE.md](docs/PROTOTYPE.md) for:
- Architecture overview and design rationale
- Implementation details and known limitations
- Production roadmap

To contribute, open an issue or pull request on GitHub.

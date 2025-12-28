(** Domain-parallel pipeline for OCaml 5.

    Build multi-stage data processing pipelines where each stage runs in
    parallel across configurable domains, connected by bounded queues with
    automatic backpressure. *)

(** Error with context about which stage failed *)
type error = { stage_name : string; message : string; exn : exn option }

(** Convert an error to a human-readable string *)
val error_to_string : error -> string

(** Pipeline builder type. The type parameters track the input and output types.
*)
type ('input, 'output) t

(** Result of pipeline execution *)
type 'a result = Ok of 'a list | Error of error list | Cancelled

(** Create a pipeline from a source function that produces items *)
val source : (unit -> 'a list) -> ('a, 'a) t

(** Add a processing stage to the pipeline.

    @param name Name of the stage (used in error messages)
    @param parallelism Number of parallel workers for this stage (default: 1)
    @param fn Function to process each item
    @param prev Previous pipeline stage
    @raise Invalid_argument if parallelism < 1 *)
val stage :
  name:string ->
  ?parallelism:int ->
  ('a -> 'b) ->
  ('input, 'a) t ->
  ('input, 'b) t

(** Add a sink stage that consumes items (no output).

    @param fn Function to consume each item
    @param prev Previous pipeline stage *)
val sink : ('a -> unit) -> ('input, 'a) t -> ('input, unit) t

(** Execute the pipeline and collect results.

    @param queue_capacity Capacity of queues between stages (default: 100)
    @param on_error Callback called for each error (default: ignore)
    @return
      [Ok results] on success, [Error errors] on failure, [Cancelled] if
      cancelled *)
val run :
  ?queue_capacity:int ->
  ?on_error:(error -> unit) ->
  ('input, 'output) t ->
  'output result

(** Execute the pipeline and raise on error.

    Convenience function that calls [run] and raises [Failure] if the pipeline
    fails or is cancelled.

    @param queue_capacity Capacity of queues between stages (default: 100)
    @raise Failure if the pipeline fails or is cancelled *)
val run_exn : ?queue_capacity:int -> ('input, 'output) t -> 'output list

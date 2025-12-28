(** Domain-parallel pipeline for OCaml 5.

    Build multi-stage data processing pipelines where each stage runs in
    parallel across configurable domains, connected by bounded queues with
    automatic backpressure. *)

(** Cancellation token for cooperative shutdown *)
module Cancel = struct
  type t = {
    mutable cancelled : bool;
    mutex : Mutex.t;
    condition : Condition.t;
  }

  let create () =
    {
      cancelled = false;
      mutex = Mutex.create ();
      condition = Condition.create ();
    }

  let cancel t =
    Mutex.lock t.mutex;
    t.cancelled <- true;
    Condition.broadcast t.condition;
    Mutex.unlock t.mutex
  [@@warning "-32"]
  (* Unused in current API; reserved for future cancellation support *)

  let is_cancelled t =
    Mutex.lock t.mutex;
    let result = t.cancelled in
      Mutex.unlock t.mutex;
      result
end

(** Error with context about which stage failed *)
type error = { stage_name : string; message : string; exn : exn option }

let error_to_string e =
  match e.exn with
  | None -> Printf.sprintf "[%s] %s" e.stage_name e.message
  | Some exn ->
    Printf.sprintf "[%s] %s: %s" e.stage_name e.message (Printexc.to_string exn)

(** Internal stage configuration *)
type stage_config = { name : string; parallelism : int }

(** Pipeline builder - accumulates stages before execution *)
type ('input, 'output) t =
  | Source : (unit -> 'a list) -> ('a, 'a) t
  | Stage : {
      prev : ('input, 'a) t;
      config : stage_config;
      fn : 'a -> 'b;
    }
      -> ('input, 'b) t
  | Sink : { prev : ('input, 'a) t; fn : 'a -> unit } -> ('input, unit) t

(** Create a pipeline from a source function that produces items *)
let source f = Source f

(** Add a processing stage to the pipeline *)
let stage ~name ?(parallelism = 1) fn prev =
  if parallelism < 1 then invalid_arg "Pipeline.stage: parallelism must be >= 1";
  Stage { prev; config = { name; parallelism }; fn }

(** Add a sink stage that consumes items (no output) *)
let sink fn prev = Sink { prev; fn }

(** Result of pipeline execution *)
type 'a result = Ok of 'a list | Error of error list | Cancelled

(** Run a stage worker in a domain

    Workers use non-blocking pop with busy-wait to avoid OCaml 5's known issues
    with Condition.wait across domains. The 100-microsecond sleep prevents
    excessive CPU usage while polling. *)
let run_stage_worker ~name ~fn ~input_queue ~output_queue ~cancel ~errors_ref
    ~errors_mutex =
  let rec loop () =
    if Cancel.is_cancelled cancel then ()
    else
      (* Use non-blocking pop to work around cross-domain Condition.wait issues in OCaml 5 *)
      match Bounded_queue.pop_nonblock input_queue with
      | None ->
        if Bounded_queue.is_closed input_queue then ()
          (* Queue closed and empty, we're done *)
        else begin
          (* Queue is empty but not closed - sleep briefly and retry *)
          Unix.sleepf 0.0001;
          (* 100 microseconds *)
          loop ()
        end
      | Some item ->
        begin try
          let result = fn item in
            if not (Cancel.is_cancelled cancel) then (
              try Bounded_queue.push output_queue result
              with Bounded_queue.Closed ->
                let error =
                  {
                    stage_name = name;
                    message = "Output queue closed while pushing result";
                    exn = None;
                  }
                in
                  Mutex.lock errors_mutex;
                  errors_ref := error :: !errors_ref;
                  Mutex.unlock errors_mutex)
        with exn ->
          let error =
            {
              stage_name = name;
              message = "Stage function raised exception during item processing";
              exn = Some exn;
            }
          in
            Mutex.lock errors_mutex;
            errors_ref := error :: !errors_ref;
            Mutex.unlock errors_mutex
        end;
        loop ()
  in
    loop ()

(** Execute the pipeline and collect results *)
let run ?(queue_capacity = 100) ?(on_error = fun _ -> ()) pipeline =
  let cancel = Cancel.create () in
  let errors_ref = ref [] in
  let errors_mutex = Mutex.create () in

  (* Collect stages in reverse order (from sink to source) *)
  let rec collect_stages : type a b.
      (a, b) t -> stage_config list * (unit -> a list) * (Obj.t -> unit) option
      = function
    | Source f -> ([], f, None)
    | Stage { prev; config; fn = _ } ->
      let (stages, source_fn, sink_fn) = collect_stages prev in
        (config :: stages, source_fn, sink_fn)
    | Sink { prev; fn } ->
      let (stages, source_fn, _) = collect_stages prev in
      let wrapped_sink obj = fn (Obj.obj obj) in
        (stages, source_fn, Some wrapped_sink)
  in

  let (stages_rev, source_fn, sink_fn) = collect_stages pipeline in
  let stages = List.rev stages_rev in

  (* Helper: Extract stage functions from pipeline *)
  let rec get_stage_fns : type a b. (a, b) t -> (Obj.t -> Obj.t) list = function
    | Source _ -> []
    | Stage { prev; fn; config = _ } ->
      let prev_fns = get_stage_fns prev in
      let wrapped_fn obj = Obj.repr (fn (Obj.obj obj)) in
        prev_fns @ [ wrapped_fn ]
    | Sink { prev; fn = _ } -> get_stage_fns prev
  in

  (* Helper: Create queues between stages *)
  let create_queues num_stages =
    let num_queues = num_stages + 1 in
      Array.init num_queues (fun _ ->
          Bounded_queue.create ~capacity:queue_capacity)
  in

  (* Helper: Spawn workers for a single stage with coordinator *)
  let spawn_stage_with_coordinator config stage_idx queues stage_fn all_domains
      =
    let input_queue = queues.(stage_idx) in
    let output_queue = queues.(stage_idx + 1) in
    let stage_domains = ref [] in

    (* Spawn worker domains for this stage *)
    for _ = 1 to config.parallelism do
      let domain =
        Domain.spawn (fun () ->
            run_stage_worker ~name:config.name ~fn:stage_fn ~input_queue
              ~output_queue ~cancel ~errors_ref ~errors_mutex)
      in
        stage_domains := domain :: !stage_domains
    done;

    (* Spawn coordinator domain that waits for all workers and closes output queue.
       This ensures downstream stages know when no more items are coming. *)
    let coordinator =
      Domain.spawn (fun () ->
          List.iter Domain.join !stage_domains;
          Bounded_queue.close output_queue)
    in
      all_domains := coordinator :: !all_domains
  in

  (* Helper: Push source items to first queue and close it *)
  let push_source_items queues =
    let source_items = source_fn () in
      List.iter
        (fun item -> Bounded_queue.push queues.(0) (Obj.repr item))
        source_items;
      Bounded_queue.close queues.(0)
  in

  (* Pipeline execution model:
     - Source produces all items into the first queue
     - Each stage processes items from input queue to output queue with coordinators
     - Final results collected from the last queue or processed by sink *)

  (* Type-erased execution using Obj to handle heterogeneous stage types at runtime.
     The public API maintains type safety through GADTs. *)
  let run_typed : type a b. (a, b) t -> b list =
   fun pipeline ->
    match stages with
    | [] ->
      (* No stages, just return source items *)
      let items = source_fn () in
        Obj.magic items
    | _ -> (
      let queues = create_queues (List.length stages) in
      let stage_fns = Array.of_list (get_stage_fns pipeline) in
      let all_domains = ref [] in

      (* Spawn workers with coordinators for each stage *)
      List.iteri
        (fun i config ->
          spawn_stage_with_coordinator config i queues stage_fns.(i) all_domains)
        stages;

      (* Push source items and close first queue *)
      push_source_items queues;

      (* Handle final output based on whether we have a sink *)
      match sink_fn with
      | Some sink_fn ->
        (* Pipeline ends with a sink - wait for all stages, then process sink *)
        List.iter Domain.join !all_domains;

        let final_queue = queues.(Array.length queues - 1) in
        let rec process_sink () =
          match Bounded_queue.pop final_queue with
          | None -> ()
          | Some item ->
            sink_fn item;
            process_sink ()
        in
          process_sink ();
          Obj.magic [] (* Sink returns unit *)
      | None ->
        (* Pipeline returns results - collect them concurrently *)
        let final_queue = queues.(Array.length queues - 1) in
        let results = ref [] in
        let collector_mutex = Mutex.create () in

        (* Start a collector domain to consume from the final queue while workers run.
           This prevents deadlock: if we wait for workers before collecting and the
           final queue fills up, workers will block on push and never complete. *)
        let collector =
          Domain.spawn (fun () ->
              let rec collect () =
                match Bounded_queue.pop_nonblock final_queue with
                | None ->
                  if Bounded_queue.is_closed final_queue then ()
                  else begin
                    Unix.sleepf 0.0001;
                    collect ()
                  end
                | Some item ->
                  Mutex.lock collector_mutex;
                  results := Obj.obj item :: !results;
                  Mutex.unlock collector_mutex;
                  collect ()
              in
                collect ())
        in

        (* Wait for all stage coordinators to complete *)
        List.iter Domain.join !all_domains;

        (* Wait for collector to finish *)
        Domain.join collector;

        List.rev !results)
  in

  try
    let results = run_typed pipeline in
      if !errors_ref <> [] then (
        let errors = List.rev !errors_ref in
          List.iter (fun e -> on_error e) errors;
          Error errors)
      else Ok results
  with
  | Bounded_queue.Closed -> Cancelled
  | Invalid_argument msg ->
    let error =
      {
        stage_name = "pipeline";
        message = "Invalid argument during pipeline execution: " ^ msg;
        exn = None;
      }
    in
      on_error error;
      Error [ error ]
  | exn ->
    let error =
      {
        stage_name = "pipeline";
        message = "Unexpected error in pipeline coordination";
        exn = Some exn;
      }
    in
      on_error error;
      Error [ error ]

(** Convenience: run and extract results or raise *)
let run_exn ?queue_capacity pipeline =
  match run ?queue_capacity pipeline with
  | Ok results -> results
  | Error errors ->
    let error_count = List.length errors in
    let msg = String.concat "\n" (List.map error_to_string errors) in
      failwith
        (Printf.sprintf "Pipeline failed with %d error(s):\n%s" error_count msg)
  | Cancelled -> failwith "Pipeline execution was cancelled"

(** Simple example demonstrating the domain-parallel pipeline API.

    This example processes a list of numbers through multiple stages: 1. Source:
    Generate numbers 1-20 2. Stage "double": Multiply each number by 2 (4
    parallel workers) 3. Stage "square": Square each number (2 parallel workers)
    4. Collect results *)

open Domain_pipeline

let () =
  Printf.printf "=== Domain-Parallel Pipeline Demo ===\n\n";

  (* Mutex to synchronize printing across domains *)
  let print_mutex = Mutex.create () in
  let safe_print fmt =
    Printf.ksprintf
      (fun s ->
        Mutex.lock print_mutex;
        Printf.printf "%s%!" s;
        Mutex.unlock print_mutex)
      fmt
  in

  (* Build the pipeline *)
  let pipeline =
    Pipeline.source (fun () ->
        safe_print "[source] Generating numbers 1-20\n";
        List.init 20 (fun i -> i + 1))
    |> Pipeline.stage ~name:"double" ~parallelism:4 (fun n ->
        (* Simulate some work *)
        Unix.sleepf 0.01;
        let result = n * 2 in
          safe_print "[double] %d -> %d (domain %d)\n" n result
            (Domain.self () :> int);
          result)
    |> Pipeline.stage ~name:"square" ~parallelism:2 (fun n ->
        Unix.sleepf 0.01;
        let result = n * n in
          safe_print "[square] %d -> %d (domain %d)\n" n result
            (Domain.self () :> int);
          result)
  in

  Printf.printf
    "Running pipeline with 4 workers for 'double', 2 for 'square'...\n\n%!";

  (* Run the pipeline *)
  let start_time = Unix.gettimeofday () in
  let results = Pipeline.run_exn ~queue_capacity:10 pipeline in
  let elapsed = Unix.gettimeofday () -. start_time in

  Printf.printf "\n=== Results ===\n";
  Printf.printf "Processed %d items in %.3f seconds\n" (List.length results)
    elapsed;
  Printf.printf "Output: [%s]\n"
    (String.concat "; " (List.map string_of_int results));

  (* Verify correctness: (n * 2)^2 for n in 1..20 *)
  let expected =
    List.init 20 (fun i ->
        let n = i + 1 in
          n * 2 * (n * 2))
  in
  let sorted_results = List.sort compare results in
  let sorted_expected = List.sort compare expected in
    if sorted_results = sorted_expected then
      Printf.printf "Results match expected values\n"
    else Printf.printf "Results do not match expected values!\n"

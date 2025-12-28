(** Bounded queue with backpressure for cross-domain communication.

    When the queue is full, [push] blocks until space is available. When the
    queue is empty, [pop] blocks until an item is available. Supports graceful
    shutdown via [close]. *)

type 'a t = {
  buffer : 'a option array;
  capacity : int;
  mutable head : int;
  mutable tail : int;
  mutable count : int;
  mutable closed : bool;
  mutex : Mutex.t;
  not_empty : Condition.t;
  not_full : Condition.t;
}

exception Closed

let create ~capacity =
  if capacity <= 0 then
    invalid_arg "Bounded_queue.create: capacity must be positive";
  {
    buffer = Array.make capacity None;
    capacity;
    head = 0;
    tail = 0;
    count = 0;
    closed = false;
    mutex = Mutex.create ();
    not_empty = Condition.create ();
    not_full = Condition.create ();
  }

let push t item =
  Mutex.lock t.mutex;
  (* Wait while queue is full and not closed *)
  while t.count = t.capacity && not t.closed do
    Condition.wait t.not_full t.mutex
  done;
  if t.closed then begin
    Mutex.unlock t.mutex;
    raise Closed
  end;
  t.buffer.(t.tail) <- Some item;
  t.tail <- (t.tail + 1) mod t.capacity;
  t.count <- t.count + 1;
  Condition.signal t.not_empty;
  Mutex.unlock t.mutex

let pop t =
  Mutex.lock t.mutex;
  (* Wait while queue is empty and not closed *)
  while t.count = 0 && not t.closed do
    Condition.wait t.not_empty t.mutex
  done;
  if t.count = 0 && t.closed then begin
    Mutex.unlock t.mutex;
    None
  end
  else begin
    let item = t.buffer.(t.head) in
      t.buffer.(t.head) <- None;
      t.head <- (t.head + 1) mod t.capacity;
      t.count <- t.count - 1;
      Condition.signal t.not_full;
      Mutex.unlock t.mutex;
      item
  end

(** Non-blocking pop for cross-domain use where blocking would cause issues.
    Returns immediately with None if queue is empty, allowing caller to
    implement custom waiting logic (e.g., sleep and retry). *)
let pop_nonblock t =
  Mutex.lock t.mutex;
  if t.count > 0 then begin
    let item_opt = t.buffer.(t.head) in
      t.buffer.(t.head) <- None;
      t.head <- (t.head + 1) mod t.capacity;
      t.count <- t.count - 1;
      Condition.signal t.not_full;
      Mutex.unlock t.mutex;
      item_opt
  end
  else if t.closed then begin
    Mutex.unlock t.mutex;
    None
  end
  else begin
    Mutex.unlock t.mutex;
    None
  end

let close t =
  Mutex.lock t.mutex;
  t.closed <- true;
  (* Wake up all waiting threads *)
  Condition.broadcast t.not_empty;
  Condition.broadcast t.not_full;
  Mutex.unlock t.mutex

let is_closed t =
  Mutex.lock t.mutex;
  let result = t.closed in
    Mutex.unlock t.mutex;
    result

let length t =
  Mutex.lock t.mutex;
  let result = t.count in
    Mutex.unlock t.mutex;
    result

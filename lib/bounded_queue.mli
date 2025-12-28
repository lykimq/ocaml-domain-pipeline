(** Bounded queue with backpressure for cross-domain communication.

    When the queue is full, [push] blocks until space is available. When the
    queue is empty, [pop] blocks until an item is available. Supports graceful
    shutdown via [close]. *)

type 'a t

exception Closed

(** Create a bounded queue with the given capacity.
    @raise Invalid_argument if capacity <= 0 *)
val create : capacity:int -> 'a t

(** Push an item into the queue. Blocks if the queue is full.
    @raise Closed if the queue has been closed *)
val push : 'a t -> 'a -> unit

(** Pop an item from the queue. Blocks if the queue is empty. Returns [None] if
    the queue is closed and empty. *)
val pop : 'a t -> 'a option

(** Close the queue. After closing, no more items can be pushed. All waiting
    threads are woken up. *)
val close : 'a t -> unit

(** Check if the queue is closed. *)
val is_closed : 'a t -> bool

(** Get the current number of items in the queue. *)
val length : 'a t -> int

(** Non-blocking pop for cross-domain use where blocking would cause issues.
    Returns [None] immediately if queue is empty, even if not closed. Useful in
    polling loops with explicit sleep to avoid Condition.wait across domains. *)
val pop_nonblock : 'a t -> 'a option

**Coalesce Design Notes**

**January 21, 2026: Execution and State**

The executor represents a pipeline as a tree of serial and parallel groups.
Associative-array keys hold each node's state. A Kubernetes Job completion
updates that state, propagates completion toward the root, and makes new
steps runnable. A watch includes newly created Jobs through their run label.

Throttling a parallel group requires tracking the number of active children.
A child group becomes active when its first descendant starts, and remains
active until the group completes. This treats a sequence of setup, work, and
cleanup as one unit for the enclosing group's concurrency limit.

A possible extension lets a step prepare resources before creating its Job.
The pipeline can manage Kubernetes resources directly; that does not require
moving resource allocation into the metadata server. Resource pooling and
late binding remain design questions, not guarantees of the executor.

The server records runs and Jobs in PostgreSQL and stores logs through Stow.
It preserves the execution record; the Zsh executor controls execution.

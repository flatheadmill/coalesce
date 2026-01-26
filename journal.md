**Coalesce Programmer's Journal**

---

**Wed Jan 21 01:54:00 PM CST 2026 - Naming, Throttling, and Functions**

The `--sync` flag has to go. Every time I read `sync:0` and `sync:1` in the case statement it hurts. The flag means "I am a container for other jobs, not a job myself" but "sync" suggests synchronization, waiting, coordination. It's sitting right next to `--parallel` which makes the confusion worse.

We brainstormed replacements. Stage, phase, channel, lane, cohort, batch, salvo, sortie, tranche, pulse. Alan wanted words that mean "bolus of work" - a chunk moving through a system. No decision yet; it'll settle as we work. Left a BOTTLE marker so we can retrieve the full list later.

Throttled parallelism came up. The journal mentions "run 50 jobs but max 5 at a time" and the current code just runs everything in parallel. The fix is straightforward once you see it: the parallel sync node gets a `max` parameter, we track `in_progress` in the associative array, increment when a child gets `:ran`, decrement when it gets `:over`. If max is unspecified, set it to some huge integer so the comparison is always the same - no special-casing unlimited.

The tricky bit was realizing `:ran` needs to exist for sync nodes too, not just pods. When the first descendant of a stage starts, the stage itself is "in progress" for throttling purposes. The reserve-PVC/run-3-hours/copy/release stage is one unit from the throttle's perspective.

We also talked about letting steps specify a function to call before creating the job. The function might query kubectl for an available PVC, claim it by labeling, do whatever setup. But for state tracking, there's always a Kubernetes Job - even if it's just `alpine true`. The watch loop doesn't know about the function; it sees a Job complete. The function is pre-flight logic that runs inline when the step becomes runnable.

This is pushing coalesce toward proper zshctl structure: `coalesce run` and `coalesce dag` as subcommands, shared functions in `share/coalesce/command.zsh`, the `_coalesce_dag_json` code that's currently dead after an `exit` becomes the dag command. The single-file prototype era is ending.

---

**Wed Jan 21 01:26:00 PM CST 2026 - Resuming After Hiatus**

Alan returned to Coalesce after several months away. During that time he completed a "once and for all" rewrite of zshctl and examplectl, answering questions like "what is the fastest way to slurp a file in Zsh" and "how do you evaluate something in the user's dynamic scope without polluting the namespace." The foundation work was necessary before building more on Zsh, and now bin/coalesce sits atop that solid base.

The core executor works. The DAG traversal, the kubectl watch loop that feeds itself when new jobs are created, the nested sync points with parallel and serial execution - all proven with the fanout test case. The mTLS connection to PostgreSQL is working. The pieces are in place.

What changed in this conversation was a rethinking of boundaries. Alan had been working on MarginalJob - privileged Kubernetes Jobs that configure nodes with nsenter and then disappear - and stumbled on the revelation that the Kubernetes Storage SIG ships example DaemonSets that run privileged forever. The same people who publish security best practices tell you to run as root with hostPID. "You can do that? People will still talk to you after you do that?"

This broke a mental partition. Alan had been carrying baggage about what layers should do what. Cloud-init for node configuration, pods for application work, Go servers for Kubernetes resource management. But if MarginalJob can kubectl its way through node configuration and disappear, why can't a Coalesce pipeline just kubectl label a PVC and claim it directly?

We had designed a PVC pool managed by the Go server - acquire via curl, release via curl, PostgreSQL tracking claims. It felt like the "right" architecture. But that architecture was shaped by a hobgoblin, an assumption that Zsh pipelines shouldn't fuss with Kubernetes directly. Once that partition dissolved, the whole approach came into question.

The new thinking is simpler. For the 50-states-on-5-machines scenario, the pipeline itself can kubectl create the work PVCs at the start, claim them by label as jobs need them, and kubectl delete them at the end. Kubernetes is the database. Labels are the state machine. The Zsh executor already recalculates runnable jobs on every completion - that's where late binding happens naturally.

This connects to a broader realization: Coalesce is a bike shed. Conceptually simple, which invites paint color debates about abstractions and patterns. Recognizing it as a bike shed is what lets you see the hobgoblins for what they are. Run jobs, record that they ran, store logs. Everything else is paint.

The first real use case is vulnerability scanning for SOC 2 compliance. Weekly scans need evidence they happened - not just the PRs opened when something changed, but proof the scans ran. Coalesce provides a table of runs with timestamps. The auditor asks for evidence, you show them a year of nightly runs. This doesn't need webhooks, doesn't need DAG visualization, doesn't need PVC pools. It needs: run the pipeline, record it in PostgreSQL, store logs in a bucket.

The decisions that have settled: Stow for bucket storage (native GCS auth, not S3 compatibility mode), PostgreSQL for metadata (runs, jobs, artifacts), logs go to buckets because we're never going to search them and the data is still there if we change our minds. The mTLS setup uses the +certificates role pattern in pg_hba.conf so cert users are members of a group that requires cert auth.

What remains fluid is the PVC strategy and custom runners. The current code always creates Kubernetes Jobs, but steps could be functions that run inline Zsh, or MarginalJob-style privileged operations, or calls to external services like a NeonVM builder. The runner could be pluggable. But these are future concerns - the vulnerability scan doesn't need them.

The throttled parallelism (run 50 jobs but max 5 at a time) needs to be built into the executor. Currently parallel means "all at once." The sync point should accept a count and the executor should manage the queue, launching new jobs as capacity frees up.

The code is dense Zsh. Associative arrays with compound keys like `_coalesce[coalesce.fanout.baz:kind]`. The `${(@AQ)${(z)...}}` expansions for serializing and deserializing arrays. The coproc for kubectl watch, the jq parsing into a tape array, the set -- to shift through it. Return code 1 meaning "not done yet" and 0 meaning "done" because that's how shell conditionals work. It looks impenetrable until you've read it three times, then it's obvious.

The project resumes with clarity about what matters and what's paint. Alan writes the code, developing the inklings that will let him fetch solutions from months past. I provide council. We're pair programming and he has the keyboard.

---

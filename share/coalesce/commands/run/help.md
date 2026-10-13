# desc
Execute a pipeline and watch for completion.
# arg -- < pipeline >
The path to a pipeline definition file.
# opt help
Display help for `coalesce run`.
# man
## DESCRIPTION
`coalesce run` executes a pipeline defined in a Zsh script. The pipeline script
uses the `step` function to build a directed acyclic graph of jobs. Once the
DAG is built, the executor creates Kubernetes Jobs and watches them via
`kubectl` until all jobs complete.

The pipeline script is sourced into the executor. It should define steps using
the `step` function. Steps without a `-p` flag become pods, actual Kubernetes
Jobs. Steps with a `-p` flag become tranches, containers for other steps that
control parallelism.

The executor maintains a `kubectl get jobs --watch` listener for each execution
context used in the DAG. All listeners live for the whole pipeline and feed
one FIFO. The parent updates the DAG and launches newly runnable Jobs after
each completion.

`step -c CONTEXT` selects an execution context from the supplied `KUBECONFIG`,
using `kubectl --context` for both creation and observation. The first step
without `-c` resolves kubectl's current context once; subsequent omitted
placements use that same name, including the recorded DAG. Pipelines with
every context explicit do not need a current context. All Jobs use the run's
namespace. The context says where a Job executes; any workload target it
operates on is still part of that Job's own command and credentials.

A runner using omitted placement must have a named current context in its
supplied kubeconfig. Kubernetes' in-cluster authentication fallback alone need
not provide one; the runner setup must supply that context explicitly.

```zsh
step -n calculate -c compute -- pod alpine -- echo calculating
```

The context is stored in DAG and Job properties. Kubernetes Jobs and Pods
carry it as an annotation because context names need not be label values.
The listener draft uses Linux FIFO writes, `setsid` and `zsh/system`. Remote
log transport and UI placement display remain separate integration work.

Jobs are labeled with `coalesce.flatheadmill.com/slug` and
`coalesce.flatheadmill.com/job` to identify which pipeline and step they belong
to. The slug comes from the `--slug` option passed to the root command.
## OPTIONS
> options
## EXIT STATUS
The `coalesce run` utility exits 0 when all jobs complete successfully.
## EXAMPLES
Run a simple pipeline.

```
coalesce --slug nightly-scan run pipelines/scan.coalesce.zsh
```

Run a test pipeline.

```
coalesce --slug test run test/fanout.coalesce.zsh
```
## SEE ALSO
`coalesce dag` to output the DAG structure without running.

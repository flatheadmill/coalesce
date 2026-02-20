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

The executor maintains a watch loop using `kubectl get jobs --watch`. When a
job completes, completion propagates up the DAG. The executor recalculates
which jobs are runnable given parallelism constraints and launches the next
batch. The loop continues until all steps are complete.

Jobs are labeled with `flatheadmill.github.io/slug` and
`flatheadmill.github.io/job` to identify which pipeline and step they belong
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

# desc
Execute DAG-structured pipelines on Kubernetes.
# opt help
Display help for `coalesce`.
# opt slug -- < name >
A unique identifier for this pipeline run. The slug is used to label Kubernetes
Jobs and to filter the watch stream. For CI/CD, use a unique slug per
invocation. For resumable data science pipelines, use the same slug across
resumptions.
# man
## DESCRIPTION
`coalesce` is a pipeline executor for Kubernetes. It runs DAG-structured
pipelines using Kubernetes Jobs as the execution primitive. The executor
watches jobs via `kubectl`, propagates completion through the DAG, respects
parallelism limits, and launches jobs as they become runnable.

Pipelines are defined in Zsh scripts using the `step` function. A step without
a `-p` flag becomes a pod, an actual Kubernetes Job. A step with a `-p` flag
becomes a tranche, a container for other steps that controls parallelism. The
`-p` argument specifies maximum concurrent children: `-p0` for unlimited, `-p1`
for serial, `-p2` for at most two concurrent.

The executor does not require a server to run pipelines. It creates Jobs
directly with `kubectl apply` and watches them with `kubectl get --watch`. A
separate Go server can observe and record run metadata to PostgreSQL and logs
to cloud storage, but the executor operates independently.

Coalesce exists because we were already doing the work. We had Tekton running
pipelines, and around Tekton we had glue that fought its abstractions rather
than leveraging them. When we measured the glue against what a purpose-built
executor would require, the numbers were comparable. The path of least
resistance stopped being the wrapper and became the thing itself.
## OPTIONS
> options
## COMMANDS
> commands
## SEE ALSO
`coalesce run` to execute a pipeline.

`coalesce dag` to output the DAG structure as JSON.

# desc
Output the pipeline DAG as JSON.
# arg -- < pipeline >
The path to a pipeline definition file.
# opt help
Display help for `coalesce dag`.
# man
## DESCRIPTION
`coalesce dag` parses a pipeline definition and outputs its structure as JSON
without executing any jobs. This is useful for visualization, validation, and
debugging pipeline definitions before running them.

The pipeline script is sourced and the `step` function builds the DAG in
memory. The resulting structure is emitted as a JSON array of nodes with their
children nested recursively.

Each node in the output includes its name, the path it sits under, its kind
(tranche or node), and for tranches whether parallelism is enabled. The output
can be piped to `jq` for further processing or to visualization tools.
## OPTIONS
> options
## EXAMPLES
Output the DAG for a pipeline.

```
coalesce --slug test dag test/fanout.coalesce.zsh
```

Pipe to `jq` for pretty printing.

```
coalesce --slug test dag test/fanout.coalesce.zsh | jq .
```
## SEE ALSO
`coalesce run` to execute the pipeline.

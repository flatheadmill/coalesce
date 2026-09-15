**Method**

Coalesce is an instrument while work is moving and a ledger when the work is done. The interface should make that change of custody visible: living runs answer what is happening now, while settled runs answer what happened, when, and where the evidence lives. It is not a control plane and should never make an operator translate a product metaphor back into Jobs, steps, timestamps, exits, and logs.

The first contact therefore has two tempos rather than a dashboard grid. Active runs occupy a distinct working field with the current step and elapsed time immediately legible. The archive beneath is denser and quieter, ordered like a record whose rows can be trusted. A failed state is the strongest visual word and remains expensive; running is visible through position, time, and explicit language rather than a field of competing colors.

Run detail reads as one evidence sheet. Its identity and outcome come first, followed by the declared DAG and the job attempts that fulfilled or failed it. The DAG should reveal order, nesting, and parallel work without becoming a pannable graph or pretending to explain future cross-run reuse. Job attempts remain individual rows because the database is an accreting ledger, not a summary that folds inconvenient history away.

The log is the terminal point of the argument, not a detached developer console. It keeps the run and job identity in view, distinguishes a live stream from a harvested record, and gives the text the largest uninterrupted surface in the exhibit. The operator should always know whether they are watching work or reading evidence.

The visual register is typographic and exact: a warm, quiet ground for the ledger; dark structural ink; tabular figures and monospace where values become evidence; one reserved failure color; and rules that expose grouping rather than boxes that manufacture importance. The page can have character, but its character should come from custody, sequence, and the physical weight of a durable record rather than from generic dashboard furniture.

The operator is treated as someone who already understands Kubernetes. Navigation should be shallow, labels literal, timestamps complete, keyboard focus visible, and narrow layouts reflowed rather than amputated. Empty, missing, loading, and transport failures remain plainly different states because an evidence interface loses authority the moment it blurs what it knows.

**Sources**

`cmd/web/main.go` defines the immutable facts available to the exhibit: run identity, job attempts, current DAG, stored logs, and advisory streams.

The former product in `ui/src` (preserved in Git history) demonstrated a useful custody distinction between living work and the settled ledger without binding this exhibit to its presentation.

The Coalesce rationale establishes the reader as a cluster operator seeking proof rather than another pipeline platform, while levels 012 and 013 place logs, deposited pages, versioned DAGs, and reused pod evidence on one future continuum.

The design distinguishes instruments from ledgers, reserves salience by channel, and requires rendered evidence before a visual claim is trusted.

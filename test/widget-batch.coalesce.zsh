# Trial apparatus for the sparkline probation: stamps a run whose duration is
# set from the bench (WIDGET_BATCH_SECONDS, default 6), so a pipeline's recent
# history can be manufactured as real recorded runs — real pods, real rows,
# controlled spread. The trick: double quotes expand at source time on the
# Mac, so the stored step carries the literal number.
function {
    step -n batch -- pod alpine -- sh -c "echo stamping; sleep ${WIDGET_BATCH_SECONDS:-6}; echo stamped"
}

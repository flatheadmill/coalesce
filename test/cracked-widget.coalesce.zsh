# The widget that fails inspection — a red row for the run table, on purpose.
function {
    step -n inspector -- pod alpine -- sh -c 'echo "inspecting widget"; echo "crack found in widget"; exit 1'
}

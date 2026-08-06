# The press stamps a widget a second for three minutes — a running row long
# enough to watch, a log that streams for the tail path.
function {
    step -n press -- pod alpine -- sh -c 'for n in $(seq 1 180); do echo "stamped widget $n"; sleep 1; done'
}

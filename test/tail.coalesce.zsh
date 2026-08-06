function {
    step -n ticker -- pod alpine -- sh -c 'for n in $(seq 1 120); do echo "tick $n"; sleep 1; done'
}

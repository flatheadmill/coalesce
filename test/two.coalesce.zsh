function {
    step -n one -- alpine -- sh -c 'for n in $(seq 1 10); do echo $n; sleep 1; done'
    step -n two -- alpine -- sh -c 'for n in $(seq 1 10); do echo $n; sleep 1; done'
}

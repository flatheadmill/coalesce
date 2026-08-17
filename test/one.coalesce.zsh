function {
    #step -n printer -- alpine -- sh -c 'for n in $(seq 1 3); do echo $n; sleep 1; done; exit 0'
    step -n printer -- pod alpine -- sh -c 'echo hello, world'
}

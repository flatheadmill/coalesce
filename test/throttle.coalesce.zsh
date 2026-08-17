function {
    step -n frobinate -p2
    for foo in a b c d; do
        step -n $foo -u frobinate -- alpine -- sh -c 'echo hello'
    done
}

function {
    step -n gather -- alpine -- sh -c 'echo hello'
    step -n cdl -- alpine -- sh -c 'echo hello'
    step -n fanout -p0
    step -n baz -u fanout -p1
    step -n frobinate -u fanout/baz -p0
    for foo in a b c d; do
        step -n $foo -u fanout/baz/frobinate -- alpine -- sh -c 'echo hello'
    done
    step -n fix -u fanout/baz -- alpine -- sh -c 'echo hello'
    step -n stitch -u fanout/baz -- alpine -- sh -c 'echo hello'
    step -n bar -u fanout -p1
    step -n frobinate -u fanout/bar -p2
    for foo in a b c d; do
        step -n $foo -u fanout/bar/frobinate -- alpine -- sh -c 'echo hello'
    done
    step -n stitch -u fanout/bar -- alpine -- sh -c 'echo hello'
    step -n over -- alpine -- sh -c 'echo hello'
}

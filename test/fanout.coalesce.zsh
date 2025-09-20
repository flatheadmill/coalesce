function {
    step -n gather -- alpine -- sh -c 'echo gather'
    step -n cdl -- alpine -- sh -c 'echo hello'
    step -n fanout -s -p
    step -n baz -u fanout -s
    step -n frobinate -u fanout/baz -s -p
    for foo in a b c d; do
        step -n $foo -u fanout/baz/frobinate -- alpine -- sh -c 'echo hello'
    done
    step -n fix -u fanout/baz -- alpine -- sh -c 'echo hello'
    step -n stitch -u fanout/baz -- alpine -- sh -c 'echo hello'
    step -n bar -u fanout -s
    step -n frobinate -u fanout/bar -s -p
    for foo in a b c d; do
        step -n $foo -u fanout/bar/frobinate -- alpine -- sh -c 'echo hello'
    done
    step -n stitch -u fanout/bar -- alpine -- sh -c 'echo hello'
    step -n over -- alpine -- sh -c 'echo hello'
}

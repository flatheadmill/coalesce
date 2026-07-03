function {
    step -n hello -- alpine -- sh -c 'for i in $(seq 10); do sleep 1; echo "hello $i"; done'
}

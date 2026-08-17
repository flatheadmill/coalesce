function {
    step -n one -- alpine -- sh -c 'echo one'
    step -n two -- alpine -- sh -c 'echo two'
}

function {
    step -n one harbor.example.org/example/gather -- /bin/foo
    step -n sub -s
    step -n one -u sub harbor.example.org/example/gather -- /bin/foo
    step -n two harbor.example.org/example/gather -- /bin/foo
}

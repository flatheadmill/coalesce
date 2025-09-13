function finally {
    case $build[step]:$build[env_ENV_FOO] in
    (cdl:*) slack 'cdl failed' ;;
    (fanout/0/baz/0/frobinate:c) slack 'forbinate c is acting up again' ;;
    (*) generic_slack ;;
    esac
}

function shutdown {
    case $build[image] in
    (*/example/*) pkill or something ;;
    (*postgres*) pkill -TERM postgres ;;
    esac
}

function {
    step -n gather -- -e ENV_FOO=foo -e ENV_BAR=baz harbor.example.org/example/gather
    step -n cdl -- -e ENV_FOO=foo -e ENV_BAR=baz harbor.example.org/example/cdl
    step -n fanout -s -p
    step -n baz -u fanout -s
    step -n frobinate -u fanout/baz -s -p
    for foo in a b c d; do
        step -n $foo -u fanout/baz/frobinate -- -e ENV_FOO=$foo -e ENV_BAR=baz harbor.example.org/example/frobinate
    done
    step -n fix -u fanout/baz -- harbor.example.org/example/fix -e ENV_BAR=baz
    step -n stitch -u fanout/baz -- harbor.example.org/example/stitch -e ENV_BAR=baz
    step -n bar -u fanout -s
    step -n frobinate -u fanout/bar -s -p
    for foo in a b c d; do
        step -n $foo -u fanout/bar/frobinate -- -e ENV_FOO=$foo -e ENV_BAR=baz harbor.example.org/example/frobinate
    done
    step -n stitch -u fanout/bar -- harbor.example.org/example/stitch -e ENV_BAR=baz
    step -n over -- harbor.example.org/example/over
    return
    step -e finally
    # And we can patch with -a after and -b before maybe just -a and -a '' is at the beginning.
}

#!/usr/bin/env zsh

# TODO Make `:::` configurable, so if you need `:::` as an argument use `@@@`
# instead, for example.
function _coalesce_job_yaml {
    typeset job=${1:-}
    shift
    typeset container containers=()
    typeset args=() arg image
    for arg in "${(@QA)${(z)_coalesce[${job}:args]}}" ":::"; do
        if [[ $arg = ":::" ]]; then
            eval "$(args -@ -- "${(@)args}")"
            image=${1:-}
            shift
            container=$(
                gojq --yaml-input \
                    --argjson args "$(jo \
                        name=${job##*.} \
                        image=$image \
                        cmd="$(jo  -a -- "$@" < /dev/null)" \
                 )" \
                '
                    .command = $args.cmd |
                    .image = $args.image |
                    .name = $args.name
                ' <({
                    heredoc <<'                    EOF'
                        name: null
                        image: null
                        command: []
                        env: []
                        args: []
                    EOF
                })
            )
            containers+=( $container )
        else
            args+=( $arg )
        fi
    done
    typeset name=${o_slug}-$(_coalesce_cksum $o_slug.$job)-${job##*.}
    typeset labels=$(
        jo -- flatheadmill.github.io/job=$job flatheadmill.github.io/slug=$o_slug
    )
    k8s[yaml]=$(
        gojq --yaml-input --yaml-output \
            --argjson args "$(jo \
                name=$name \
                labels=$labels \
                namespace=default \
                containers="$(jo  -a -- "${(@)containers}" < /dev/null)" \
         )" \
        '
            .metadata.name = $args.name |
            .metadata.namespace = $args.namespace |
            .metadata.labels = $args.labels |
            .spec.template.spec.containers = $args.containers
        '  <({
            heredoc <<'            EOF'
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: null
                  namespace: null
                  labels: {}
                spec:
                  backoffLimit: 0
                  ttlSecondsAfterFinished: 300
                  template:
                    spec:
                      restartPolicy: Never
                      containers: []
            EOF
        })
    )
}

function _coalesce_propagate_over {
    typeset child=${1:-}
    shift
    typeset parent=${child%.*}
    print over $child $parent
    ((_coalesce[${parent}:over]++))
    typeset queue=( "${(@AQ)${(z)_coalesce[${parent}:queue]}}" )
    if [[ $_coalesce[${parent}:over] -lt ${#queue} || $parent = coalesce ]]; then
        return
    fi
    print ">>>" $parent $_coalesce[${parent}:over] ${#queue}
    _coalesce_propagate_over $parent
}

function _coalesce_descend_jobs {
    typeset node=${1:-} queue=()
    shift
    queue=( "${(@AQ)${(z)_coalesce[${node}:queue]}}" )
    if (( ${#queue} == _coalesce[${node}:over] )); then
        return
    fi
    function {
        typeset child
        integer from=$(( _coalesce[${node}:over] + 1 ))
        integer to=$(( _coalesce[${node}:started] + 1 ))
        print $parent from: $from to: $to
        while (( from < to )); do
            child=${node}.${queue[$from]}
            ((from++))
            case $_coalesce[${child}:kind] in
            (tranche)
                _coalesce_descend_jobs $child
                ;;
            esac
        done
    }
    function {
        typeset child
        integer offset=$(( _coalesce[${node}:started] + 1 ))
        integer outstanding=$(( _coalesce[${node}:started] - _coalesce[${node}:over] ))
        integer parallel=$_coalesce[${node}:parallel]
        print $node outstanding: $outstanding parallel: $parallel \
            offset: $offset outcome count: ${#queue}
        while (( outstanding < parallel && offset <= ${#queue} )); do
            child=${node}.${queue[$offset]}
            ((offset++))
            ((outstanding++))
            ((_coalesce[${node}:started]++))
            case $_coalesce[${child}:kind] in
            (tranche)
                _coalesce_descend_jobs $child
                ;;
            (pod)
                runnable+=( $child )
                ;;
            esac
        done
    }
}

function _coalesce_run_start_jobs {
    typeset job
    typeset -A k8s
    for job in "${(@)runnable}"; do
        (( ${+_coalesce[${job}:ran]} )) && continue
        _coalesce_job_yaml $job
        # TODO Check with app if there is a metadata.json.
        kubectl apply -f - <<< $k8s[yaml]
        _coalesce[${job}:ran]=1
    done
}

function _coalesce_run {
    typeset runnable=() tape=() outcomes=()
    typeset line key value completed outcome_count outcome
    typeset -A labels
    _coalesce_descend_jobs coalesce
    _coalesce_run_start_jobs
    integer fd child entries over
    while (( ! _coalesce[over] )); do
        coproc kubectl get jobs -l flatheadmill.github.io/slug=$o_slug \
            --watch --output-watch-events --output json
        child=${!}
        _coalesce_children+=( $child )
        exec {fd}<&p;
        coproc :
        while read -r line; do
            tape=( "${(@QA)${(z)$(
                jq -r '
                    (.object.status.conditions // [{type:"Pending"}]) as $conditions |
                    [
                        (.object.metadata.labels | length),
                        (.object.metadata.labels | to_entries[] | (.key, .value)),
                        ($conditions | length),
                        ($conditions[] | .type)
                    ] | @sh
                '<<< $line
            )}}" )
            set -- "${(@)tape}"
            entries=${1:-}
            shift
            while (( entries-- )); do
                key=${1:-} value=${2:-}
                shift 2
                labels[$key]=$value
            done
            outcome_count=${1:-}
            shift
            outcomes=( "${(@)@[1,$outcome_count]}" )
            shift $outcome_count
            completed=
            for outcome in "${(@)outcomes}"; do
                case $outcome in
                (Pending|SuccessCriteriaMet) ;;
                (Failed) ;;
                (Complete)
                    completed=$labels[flatheadmill.github.io/job]
                    print complete $completed
                    _coalesce[${completed}:over]=1
                    ;;
                esac
            done
            if [[ ! -z $completed ]]; then
                _coalesce_propagate_over $completed
                runnable=()
                _coalesce_descend_jobs coalesce
                if (( ${#runnable} )); then
                    _coalesce_run_start_jobs
                else
                    over=1
                    for key in "${(@k)_coalesce}"; do
                        if (( ! ${+_coalesce[${key%%:*}:over]} )); then
                            over=0
                            break
                        fi
                    done
                    if (( over )); then
                        kill $child
                        wait $child
                        _coalesce[over]=1
                        break
                    fi
                fi
            fi
        done <&${fd}
        _coalesce_children=()
    done
    print exiting
}

function :args:coalesce:run {
    eval "$(args -bx h,help -- "$@")"
}

function :execute:coalesce:run {
    function TRAPTERM {
        typeset child
        _coalesce[over]=1
        for child in "${(@)_coalesce_children}"; do
            kill $child
        done
    }
    _coalesce_init
    source $1
    _coalesce_run
}

#!/usr/bin/env zsh

function pod {
    typeset container containers=()
    typeset args=() arg image
    for arg in "$@" ":::"; do
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
    o_yaml=$(
        gojq --yaml-input --yaml-output \
            --argjson args "$(jo \
                containers="$(jo  -a -- "${(@)containers}" < /dev/null)" \
         )" \
        '
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
                    metadata:
                      labels: {}
                    spec:
                      restartPolicy: Never
                      containers: []
            EOF
        })
    )
}

# Builds Job YAML from the step's stored arguments using gojq and jo. The
# `:::` separator splits arguments into multiple containers — everything
# between separators becomes one container definition. This is how a step
# declares a multi-container pod (main + sidecar).
#
# The job name is slug + CRC32 hash + leaf name, making it deterministic.
# Re-running a pipeline with the same slug produces the same job names, which
# is how ON CONFLICT resume works in the cubbyhole.
#
# TODO Make `:::` configurable, so if you need `:::` as an argument use `@@@`
# instead, for example.
function _coalesce_job_yaml {
    typeset job=${1:-}
    shift
    typeset o_yaml
    if ! "${(@QA)${(z)_coalesce[${job}:args]}}"; then
        print -u2 "cannot create $job: Job manifest command failed"
        return 1
    fi
    typeset name=${o_slug}-$(_coalesce_cksum $o_slug.$job)-${job##*.}
    if (( ${#name} > 63 )); then
        print -u2 "cannot create $job: derived Job name '$name' is ${#name} characters; Kubernetes labels allow 63"
        return 1
    fi
    typeset labels=$(
        jo -- -s coalesce.flatheadmill.com/job=$job \
            -s coalesce.flatheadmill.com/slug=$o_slug
    )
    # imagePullSecrets is optional: emitted only when COALESCE_IMAGE_PULL_SECRET
    # names the secret, so public-image and OrbStack runs stay untouched while a
    # private-registry cluster can pull.
    k8s[yaml]=$(
        gojq --yaml-input --yaml-output \
            --arg cluster "$_coalesce[${job}:cluster]" \
            --argjson args "$(jo \
                name=$name \
                labels=$labels \
                namespace=$o_namespace \
         )" \
        '
            .metadata.name = $args.name |
            .metadata.namespace = $args.namespace |
            .metadata.labels = $args.labels |
            .spec.template.metadata.labels = $args.labels |
            .metadata.annotations["coalesce.flatheadmill.com/cluster"] = $cluster |
            .spec.template.metadata.annotations["coalesce.flatheadmill.com/cluster"] = $cluster
        '  <(printf %s "$o_yaml")
    )
}

# A skipped node never launched, so it has no job record to update. Mark its
# whole subtree terminal in memory; this keeps the counter-scheduler honest
# without turning "skipped" into stored evidence that would poison a resume.
function _coalesce_skip_tree {
    typeset node=${1:-} child
    typeset queue=()
    _coalesce[${node}:status]=skipped
    case $_coalesce[${node}:kind] in
    (tranche)
        queue=( "${(@AQ)${(z)_coalesce[${node}:queue]}}" )
        _coalesce[${node}:started]=${#queue}
        _coalesce[${node}:over]=${#queue}
        for child in "${(@)queue}"; do
            _coalesce_skip_tree ${node}.${child}
        done
        ;;
    (pod)
        _coalesce[${node}:over]=1
        ;;
    esac
    print skipped $node
}

# A serial tranche declares that later siblings depend on the failed child.
# They never launch. A parallel tranche never calls this function: its queued
# siblings are independent and continue to drain under the ordinary throttle.
function _coalesce_skip_serial_tail {
    typeset parent=${1:-} child
    typeset queue=( "${(@AQ)${(z)_coalesce[${parent}:queue]}}" )
    integer offset=$(( _coalesce[${parent}:started] + 1 ))
    while (( offset <= ${#queue} )); do
        child=${parent}.${queue[$offset]}
        ((offset++))
        ((_coalesce[${parent}:started]++))
        ((_coalesce[${parent}:over]++))
        _coalesce_skip_tree $child
    done
}

# Terminal state propagates along the same structural edges as completion. A
# parent becomes failed if any child failed, but does not become terminal until
# every independent child has drained (or every dependent tail node was
# skipped). The root's status is the run-level rollup.
function _coalesce_propagate_terminal {
    typeset child=${1:-}
    typeset terminal_status=${2:-}
    typeset parent=${child%.*}
    typeset parent_status=completed
    typeset queue=( "${(@AQ)${(z)_coalesce[${parent}:queue]}}" )

    ((_coalesce[${parent}:over]++))
    if [[ $terminal_status = failed ]]; then
        _coalesce[${parent}:failed]=1
        if (( _coalesce[${parent}:parallel] == 1 )); then
            _coalesce_skip_serial_tail $parent
        fi
    fi
    if (( _coalesce[${parent}:over] < ${#queue} )); then
        return
    fi

    (( ${+_coalesce[${parent}:failed]} )) && parent_status=failed
    _coalesce[${parent}:status]=$parent_status
    print terminal $parent $parent_status
    if [[ $parent = coalesce ]]; then
        _coalesce[over]=1
        return
    fi
    _coalesce_propagate_terminal $parent $parent_status
}

function _coalesce_mark_terminal {
    typeset node=${1:-}
    typeset terminal_status=${2:-}
    (( ${+_coalesce[${node}:status]} )) && return 1
    _coalesce[${node}:status]=$terminal_status
    _coalesce[${node}:over]=1
    _coalesce_propagate_terminal $node $terminal_status
}

# The scheduling heart. Called after each completion to find newly runnable
# work. Two anonymous functions divide the logic:
#
# First function: re-descend into tranches that are already started but not
# yet complete. A tranche with 4 children and 2 over has work to check
# inside. This catches the case where a completion inside a nested tranche
# frees up the next sibling.
#
# Second function: start new work at this level, respecting the parallel
# limit. The outstanding count (started minus over) is compared against the
# parallel cap. Tranches are descended into immediately. Pods are appended
# to the runnable array for _coalesce_run_start_jobs to launch.
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

# Launch runnable pods. The :ran check enables resume — if we loaded state
# from PostgreSQL at startup, already-completed jobs are skipped. A rejected
# Job is a failed node, not a reason to wait forever for a watch event that can
# never arrive. Drain immediate failures here so independent work still runs
# and a run with no successfully created Jobs can unwind without waiting for
# an event.
function _coalesce_run_start_jobs {
    typeset job current=()
    typeset -A k8s
    integer create_failed
    while (( ${#runnable} )); do
        current=( "${(@)runnable}" )
        runnable=()
        create_failed=0
        for job in "${(@)current}"; do
            (( ${+_coalesce[${job}:ran]} )) && continue
            _coalesce[${job}:ran]=1
            curl -s -X POST "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${job}" \
                -H 'Content-Type: application/json' \
                -d "$(jo -- -s cluster="$_coalesce[${job}:cluster]")" > /dev/null
            # TODO Check with app if there is a metadata.json.
            if _coalesce_job_yaml $job &&
                    kubectl --context "$_coalesce[${job}:cluster]" apply \
                        --namespace $o_namespace -f - <<< $k8s[yaml]; then
                continue
            fi
            create_failed=1
            if _coalesce_mark_terminal $job failed; then
                print failed $job
                curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${job}" \
                    -H 'Content-Type: application/json' \
                    -d '{"status": "failed"}' > /dev/null
            fi
        done
        (( create_failed && ! _coalesce[over] )) || break
        _coalesce_descend_jobs coalesce
    done
}

function _coalesce_interrupt {
    trap "" TERM
    _coalesce_run_status=interrupted
    _coalesce_stop_children
    [[ -n $watch_fd ]] && exec {watch_fd}>&-
    watch_fd=
    [[ -n $watch_dir ]] && rm -rf -- "$watch_dir"
    watch_dir=
    _coalesce_finish_run
    # exit need not run the always block; this trap has cleaned up itself.
    exit 143
}

# Stash finished containers while their pods still exist. The execution context
# supplies the logs; the central server supplies the recorded job timestamp.
function _coalesce_stash_logs {
    setopt localoptions pipefail
    typeset job=$1 cluster=$2 entries line pod container
    typeset -a entry
    entries=$(
        kubectl --context "$cluster" --namespace "$o_namespace" get pods \
            -l "coalesce.flatheadmill.com/slug=$o_slug,coalesce.flatheadmill.com/job=$job" -o json |
        jq -r '.items[] | .metadata.name as $pod |
            (.status.initContainerStatuses[]?, .status.containerStatuses[]?) |
            select(.state.terminated != null) | [$pod, .name] | @sh'
    ) || return
    [[ -n $entries ]] || return 1
    for line in "${(@f)entries}"; do
        entry=( "${(@QA)${(z)line}}" )
        pod=$entry[1] container=$entry[2]
        kubectl --context "$cluster" --namespace "$o_namespace" logs "$pod" \
            -c "$container" > "$watch_dir/container.log" || return
        curl --fail --connect-timeout 10 --max-time 120 \
            -X POST "${_coalesce_url}/api/${o_namespace}/containers/${o_slug}/${job}/${container}" \
            -H 'Content-Type: text/plain' --data-binary "@$watch_dir/container.log" \
            > /dev/null || return
        print "stashed $job/$container"
    done
}

# One listener per context feeds the FIFO for the whole pipeline. The parent
# alone changes the DAG. Every placement is a resolved context name from the
# supplied KUBECONFIG.
function _coalesce_run {
    setopt localoptions localtraps
    typeset runnable=() tape=() key cluster job terminal line
    typeset -aU clusters=()
    typeset watch_dir= watch_fd=
    for key in "${(@k)_coalesce}"; do
        [[ $key = *:cluster ]] && clusters+=( "$_coalesce[$key]" )
    done
    if (( ! ${#clusters} )); then
        _coalesce[coalesce:status]=completed
        return 0
    fi

    trap '_coalesce_interrupt' TERM
    {
        zmodload zsh/system || return 1
        watch_dir=$(mktemp -d "${TMPDIR:-/tmp}/coalesce.XXXXXXXX") || return 1
        mkfifo "$watch_dir/events" || return 1
        # Linux permits this open without a writer. It also means EOF cannot
        # tell us a listener died; that remains a question for the walkthrough.
        sysopen -rw -o cloexec -u watch_fd "$watch_dir/events" || return 1
        for cluster in "${(@)clusters}"; do
            function {
                setopt localoptions localtraps
                unsetopt multibyte
                typeset child= raw= out= before=$! record line
                # The parent's list trap resets to default on this fork.
                # Our list trap returns from this anonymous function, through
                # its always block, even from a read or full-FIFO syswrite.
                trap 'trap "" TERM; return 143' TERM
                trap '' PIPE
                {
                    exec {watch_fd}>&-
                    sysopen -w -o cloexec -u out "$watch_dir/events" || return 1
                    while true; do
                        before=$!
                        # List the current Jobs, then watch for changes.
                        coproc setsid kubectl --context "$cluster" get jobs \
                            --namespace $o_namespace -l coalesce.flatheadmill.com/slug=$o_slug \
                            --watch --output-watch-events --output json {out}>&-
                        child=$!
                        exec {raw}<&p || return 1
                        # Replacement closes the old stdin pipe, not the
                        # process or our saved read descriptor.
                        coproc :
                        while IFS= read -r -u $raw line; do
                            record=$(jq -r '
                                select(.type == "ADDED" or .type == "MODIFIED") |
                                .object | select(.kind == "Job") |
                                .metadata.labels["coalesce.flatheadmill.com/job"] as $job |
                                select($job | type == "string") |
                                [.status.conditions[]? | select(.status == "True") | .type] as $conditions |
                                (if $conditions | index("Failed") then "failed"
                                 elif $conditions | index("Complete") then "completed"
                                 else empty end) as $status |
                                [$job, $status] | @sh
                            ' <<< "$line" {out}>&- {raw}<&-) || break
                            [[ -n $record ]] || continue
                            # Context names need not be label values. Dollar
                            # quoting keeps even a newline inside one record.
                            record="${(qqqq)cluster} $record"$'\n'
                            # MULTIBYTE is off: count bytes including newline.
                            # One blocking Linux write <= PIPE_BUF is atomic.
                            if (( ${#record} > 4096 )); then
                                print -u2 "Job event tape exceeds PIPE_BUF: $cluster"
                                return 1
                            fi
                            syswrite -o $out "$record" || return 1
                        done
                        # Before reconnecting, signal the watch's group and
                        # its PID, wait for it, then signal the group again
                        # for any remaining helper. The direct PID covers
                        # the interval before setsid creates the group.
                        kill -TERM -- -$child 2> /dev/null
                        kill -TERM $child 2> /dev/null
                        wait $child 2> /dev/null
                        kill -TERM -- -$child 2> /dev/null
                        exec {raw}<&-
                        raw=
                        before=$!
                        child=
                        # Bound reconnect churn. TERM waits at most this
                        # second for the foreground command to finish.
                        sleep 1
                    done
                } always {
                    trap '' TERM
                    # A queued TERM can precede child=$!, but Zsh has already
                    # published the new PID before it runs the trap.
                    [[ -z $child && $! != $before ]] && child=$!
                    if [[ -n $child ]]; then
                        kill -TERM -- -$child 2> /dev/null
                        kill -TERM $child 2> /dev/null
                        wait $child 2> /dev/null
                        kill -TERM -- -$child 2> /dev/null
                    fi
                    [[ -n $raw ]] && exec {raw}<&-
                    [[ -n $out ]] && exec {out}>&-
                }
            } &
            # tini -g reaches the listener even before this append. A signal
            # sent only to the parent in this interval remains a draft edge.
            _coalesce_children+=( $! )
        done

        _coalesce_descend_jobs coalesce
        _coalesce_run_start_jobs
        while (( ! _coalesce[over] )); do
            IFS= read -r -u $watch_fd line || return 1
            tape=( "${(@QA)${(z)line}}" )
            (( ${#tape} == 3 )) || continue
            cluster=$tape[1] job=$tape[2] terminal=$tape[3]
            [[ $_coalesce[${job}:kind] = pod &&
               $_coalesce[${job}:cluster] = $cluster &&
               ${_coalesce[${job}:ran]:-0} = 1 ]] || continue
            [[ $terminal = (failed|completed) ]] || continue
            if _coalesce_mark_terminal "$job" "$terminal"; then
                print $terminal $job
                _coalesce_stash_logs "$job" "$cluster" ||
                    print -u2 -- "unable to stash logs for $job on $cluster"
                curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${job}" \
                    -H 'Content-Type: application/json' \
                    -d "$(jo status=$terminal)" > /dev/null
                (( _coalesce[over] )) && break
                _coalesce_descend_jobs coalesce
                _coalesce_run_start_jobs
            fi
        done
    } always {
        _coalesce_stop_children
        [[ -n $watch_fd ]] && exec {watch_fd}>&-
        [[ -n $watch_dir ]] && rm -rf -- "$watch_dir"
    }
    print exiting
}

function :args:run {
    eval "$(args s,slug N,namespace -bx h,help -- "$@")"
}

function _coalesce_finish_run {
    (( _coalesce_run_started )) || return
    curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/runs/${o_slug}" \
        -H 'Content-Type: application/json' \
        -d "$(jo status=$_coalesce_run_status)" > /dev/null || return
    _coalesce_run_started=0
}

function :execute:run {
    [[ -v o_slug ]] || abend 'slug is required'
    [[ -v o_namespace ]] || o_namespace=default
    typeset -g _coalesce_url=${COALESCE_URL:-http://coalesce.default.svc.cluster.local}
    _coalesce_run_started=0
    _coalesce_run_status=running
    _coalesce_init
    source $1
    # POST the DAG and start a run in the cubbyhole before launching jobs.
    # Invoke the dag subcommand rather than calling functions directly —
    # subcommands stay in their own files so they don't all get copied
    # into branch commands.
    typeset dag_json=$($zshctl[argzero] dag -s $o_slug \
        -c "$_coalesce[coalesce:default_cluster]" $1)
    curl -s -X POST "${_coalesce_url}/api/${o_namespace}/dags/${o_slug}" \
        -H 'Content-Type: application/json' \
        -d "{\"dag\": $dag_json}" > /dev/null || abend 'unable to reach storage'
    curl -s -X POST "${_coalesce_url}/api/${o_namespace}/runs/${o_slug}" \
        -H 'Content-Type: application/json' \
        -d "$(jo pipeline="${COALESCE_PIPELINE_NAME:-$1}")" > /dev/null ||
        abend 'unable to reach storage'
    _coalesce_run_started=1
    _coalesce_run
    _coalesce_run_status=${_coalesce[coalesce:status]:-failed}
    _coalesce_finish_run || return 1
    [[ $_coalesce_run_status = completed ]]
}

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
    "${(@QA)${(z)_coalesce[${job}:args]}}"
    typeset name=${o_slug}-$(_coalesce_cksum $o_slug.$job)-${job##*.}
    typeset labels=$(
        jo -- flatheadmill.github.io/job=$job flatheadmill.github.io/slug=$o_slug
    )
    # imagePullSecrets is optional: emitted only when COALESCE_IMAGE_PULL_SECRET
    # names the secret, so public-image and OrbStack runs stay untouched while a
    # private-registry cluster (millwright on Harbor) can pull.
    k8s[yaml]=$(
        gojq --yaml-input --yaml-output \
            --argjson args "$(jo \
                name=$name \
                labels=$labels \
                namespace=$o_namespace \
         )" \
        '
            .metadata.name = $args.name |
            .metadata.namespace = $args.namespace |
            .metadata.labels = $args.labels |
            .spec.template.metadata.labels = $args.labels
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
# from PostgreSQL at startup, already-completed jobs are skipped.
function _coalesce_run_start_jobs {
    typeset job
    typeset -A k8s
    for job in "${(@)runnable}"; do
        (( ${+_coalesce[${job}:ran]} )) && continue
        _coalesce_job_yaml $job
        # TODO Check with app if there is a metadata.json.
        kubectl apply --namespace $o_namespace -f - <<< $k8s[yaml]
        _coalesce[${job}:ran]=1
        curl -s -X POST "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${job}" \
            -H 'Content-Type: application/json' -d '{}' > /dev/null
    done
}

# The event loop. After the initial descend-and-launch, it opens a kubectl
# watch as a coproc filtered by the slug label. Every job created with this
# slug appears in the watch stream automatically — the loop feeds itself.
#
# The outer while loop exists because kubectl watches can disconnect. If the
# watch drops, it restarts. The inner loop reads JSON events, parses them
# through jq into a tape (a flat array of labels and conditions), and acts
# on completions: mark done, propagate up, descend for new work, launch.
#
# The coproc pattern: open as coproc, capture PID, steal the fd, then
# immediately replace the coproc slot with a no-op so the fd stays open
# without holding the coproc. This lets us track the PID for SIGTERM cleanup.
function _coalesce_run {
    typeset runnable=() tape=() outcomes=()
    typeset line key value terminal outcome_count outcome
    typeset -A labels
    _coalesce_descend_jobs coalesce
    _coalesce_run_start_jobs
    integer fd child entries over
    while (( ! _coalesce[over] )); do
        coproc kubectl get jobs --namespace $o_namespace -l flatheadmill.github.io/slug=$o_slug \
            --watch --output-watch-events --output json
        child=${!}
        _coalesce_children+=( $child )
        exec {fd}<&p;
        coproc :
        # Each JSON event is flattened by jq into a tape: label count, then
        # key-value pairs, then condition count, then condition types. This
        # avoids calling jq multiple times per event — one parse extracts
        # everything, Zsh unpacks it positionally.
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
            terminal=
            for outcome in "${(@)outcomes}"; do
                case $outcome in
                (Pending|SuccessCriteriaMet) ;;
                (Failed)
                    typeset failed_job=$labels[flatheadmill.github.io/job]
                    if _coalesce_mark_terminal $failed_job failed; then
                        terminal=$failed_job
                        print failed $failed_job
                        curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${failed_job}" \
                            -H 'Content-Type: application/json' \
                            -d '{"status": "failed"}' > /dev/null
                    fi
                    ;;
                (Complete)
                    typeset completed_job=$labels[flatheadmill.github.io/job]
                    if _coalesce_mark_terminal $completed_job completed; then
                        terminal=$completed_job
                        print complete $completed_job
                        curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${completed_job}" \
                            -H 'Content-Type: application/json' \
                            -d '{"status": "completed"}' > /dev/null
                    fi
                    ;;
                esac
            done
            if [[ -n $terminal ]]; then
                if (( _coalesce[over] )); then
                    _coalesce_stop_children
                    break
                fi
                runnable=()
                _coalesce_descend_jobs coalesce
                if (( ${#runnable} )); then
                    _coalesce_run_start_jobs
                fi
            fi
        done <&${fd}
        _coalesce_stop_children
    done
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
    typeset dag_json=$($zshctl[argzero] dag -s $o_slug $1)
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

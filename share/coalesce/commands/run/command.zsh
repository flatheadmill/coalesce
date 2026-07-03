#!/usr/bin/env zsh

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
    # imagePullSecrets is optional: emitted only when COALESCE_IMAGE_PULL_SECRET
    # names the secret, so public-image and OrbStack runs stay untouched while a
    # private-registry cluster (millwright on Harbor) can pull.
    k8s[yaml]=$(
        gojq --yaml-input --yaml-output \
            --argjson args "$(jo \
                name=$name \
                labels=$labels \
                namespace=$o_namespace \
                pullSecret=${COALESCE_IMAGE_PULL_SECRET:-} \
                containers="$(jo  -a -- "${(@)containers}" < /dev/null)" \
         )" \
        '
            .metadata.name = $args.name |
            .metadata.namespace = $args.namespace |
            .metadata.labels = $args.labels |
            .spec.template.metadata.labels = $args.labels |
            .spec.template.spec.containers = $args.containers |
            (if ($args.pullSecret // "") != "" then .spec.template.spec.imagePullSecrets = [{ name: $args.pullSecret }] else . end)
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

# Completion propagates up the tree. When a child completes, its parent's
# :over count increments. If :over equals the queue length, the parent itself
# is complete and propagation continues upward. The root node "coalesce" stops
# the recursion — the pipeline is done when the root's children are all over.
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
    typeset line key value completed outcome_count outcome
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
            completed=
            for outcome in "${(@)outcomes}"; do
                case $outcome in
                (Pending|SuccessCriteriaMet) ;;
                (Failed)
                    # Record the failure in the cubbyhole. Failure propagation
                    # (whether to stop siblings or the run) is a policy decision
                    # for a later phase.
                    typeset failed_job=$labels[flatheadmill.github.io/job]
                    print failed $failed_job
                    curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${failed_job}" \
                        -H 'Content-Type: application/json' \
                        -d '{"status": "failed"}' > /dev/null
                    ;;
                (Complete)
                    completed=$labels[flatheadmill.github.io/job]
                    print complete $completed
                    _coalesce[${completed}:over]=1
                    curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/jobs/${o_slug}/${completed}" \
                        -H 'Content-Type: application/json' \
                        -d '{"status": "completed"}' > /dev/null
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
                    # No new runnable pods, but are we actually done? Check
                    # every node in the DAG for an :over entry. If any node
                    # lacks one, there's still outstanding work — it just
                    # hasn't completed yet.
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

function :args:run {
    eval "$(args -bx h,help -- "$@")"
}

function :execute:run {
    function TRAPTERM {
        typeset child
        _coalesce[over]=1
        for child in "${(@)_coalesce_children}"; do
            kill $child
        done
    }
    typeset -g _coalesce_url=${COALESCE_URL:-http://coalesce-web.default.svc.cluster.local}
    _coalesce_init
    source $1
    # POST the DAG and start a run in the cubbyhole before launching jobs.
    # Invoke the dag subcommand rather than calling functions directly —
    # subcommands stay in their own files so they don't all get copied
    # into branch commands.
    typeset dag_json=$($zshctl[argzero] -s $o_slug dag $1)
    curl -s -X POST "${_coalesce_url}/api/${o_namespace}/dags/${o_slug}" \
        -H 'Content-Type: application/json' \
        -d "{\"dag\": $dag_json}" > /dev/null
    curl -s -X POST "${_coalesce_url}/api/${o_namespace}/runs/${o_slug}" \
        -H 'Content-Type: application/json' \
        -d "$(jo pipeline=$1)" > /dev/null
    _coalesce_run
    # The run is over — mark it in the cubbyhole. This is the SOC 2 money
    # shot: the scan ran and completed.
    curl -s -X PUT "${_coalesce_url}/api/${o_namespace}/runs/${o_slug}" \
        -H 'Content-Type: application/json' \
        -d '{"status": "completed"}' > /dev/null
}

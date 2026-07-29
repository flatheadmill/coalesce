function zsh_step {
    eval "$(args f,func -- "$@")"
    [[ -v o_func ]] || abend "--func is required"
    typeset src=${functions[$o_func]}
    trim src
    o_yaml=$(
        gojq --yaml-input --yaml-output \
            --argjson args "$(jo \
                args="$(jo  -a -- "sh" "-c" "$src" < /dev/null)" \
         )" \
        '
            .spec.template.spec.containers[0].args = $args.args
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
                      imagePullSecrets:
                      - name: registry
                      restartPolicy: Never
                      nodeSelector:
                        node-role.kubernetes.io/coalesce: ""
                      tolerations:
                      - key: node-role.kubernetes.io/coalesce
                        operator: Exists
                        effect: NoSchedule
                      containers:
                      - name: build
                        image: harbor.example.org/example/millwright
                        imagePullPolicy: Always
                        args: []
                        securityContext:
                          allowPrivilegeEscalation: true
                          seccompProfile:
                            type: Unconfined
                          appArmorProfile:
                            type: Unconfined
                        volumeMounts:
                        - name: millwright
                          mountPath: /run/example/millwright
                        - name: secret
                          readOnly: true
                          mountPath: "/run/example/secret"
                        - name: docker
                          mountPath: /run/example/docker
                      volumes:
                      - name: millwright
                        configMap:
                          name: millwright
                      - name: secret
                        secret:
                          secretName: secret
                      - name: docker
                        secret:
                          secretName: registry
                          items:
                          - key: .dockerconfigjson
                            path: config.json
            EOF
        })
    )
    print -r -- "$o_yaml"
}

# The build step, parameterized by the pull request the run was fired for.
# The __TOKENS__ are substituted from the executor's environment when this
# file is sourced, so the SHA the container builds is the PR's real head, not
# a literal frozen in the file. Values expand at source time in the executor
# shell (option A, no framework change); the body then rides into the pod
# unchanged. The env is set by whatever fires the run -- a hand-set demo today,
# the webhook receiver later.
function say_hello {
    eval `ssh-agent`
    export GITHUB_BASE_REF=__BASE_REF__
    export GITHUB_BASE_SHA=__BASE_SHA__
    export GITHUB_HEAD_REF=__HEAD_REF__
    export GITHUB_HEAD_SHA=__HEAD_SHA__
    tar -C /home/linuxbrew/.linuxbrew -zxf /run/example/millwright/millwright.tar.gz
    millwright gitconfig
    millwright sshconfig
    millwright mirror --repository __REPO__
    millwright merge --repository __REPO__
    export BUILDKITD_FLAGS="--oci-worker-no-process-sandbox --root /home/linuxbrew/.local/share/buildkit"
    export ROOTLESSKIT="rootlesskit --copy-up=/run"
    export DOCKER_CONFIG=/run/example/docker
    export XDG_RUNTIME_DIR=/home/linuxbrew/.run
    mkdir -p $XDG_RUNTIME_DIR /home/linuxbrew/.local/share/buildkit && chmod 700 $XDG_RUNTIME_DIR
    buildctl-daemonless.sh build --frontend dockerfile.v0 --local context=/git/__NAME__/main --local dockerfile=/git/__NAME__/main --output type=image,name=__IMAGE__,push=true
}

function {
    : ${GITHUB_REPOSITORY:?set GITHUB_REPOSITORY} ${GITHUB_PR_NUMBER:?set GITHUB_PR_NUMBER}
    : ${GITHUB_BASE_REF:?} ${GITHUB_BASE_SHA:?} ${GITHUB_HEAD_REF:?} ${GITHUB_HEAD_SHA:?}
    # millwright strips owner/prefix- from the repo name: example-secrets -> secrets.
    typeset name=${GITHUB_REPOSITORY#*/*-}
    typeset image=harbor.example.org/example/${name}:pr-${GITHUB_PR_NUMBER}
    typeset body=${functions[say_hello]}
    body=${body//__BASE_REF__/$GITHUB_BASE_REF}
    body=${body//__BASE_SHA__/$GITHUB_BASE_SHA}
    body=${body//__HEAD_REF__/$GITHUB_HEAD_REF}
    body=${body//__HEAD_SHA__/$GITHUB_HEAD_SHA}
    body=${body//__REPO__/$GITHUB_REPOSITORY}
    body=${body//__NAME__/$name}
    body=${body//__IMAGE__/$image}
    functions[say_hello]=$body
    step -n build -- zsh_step -f say_hello
}

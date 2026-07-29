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
            --arg action "$GITHUB_ACTION" \
            --arg repository "$GITHUB_REPOSITORY" \
            --arg pull_request "$GITHUB_PR_NUMBER" \
            --arg base_ref "$GITHUB_BASE_REF" \
            --arg base_sha "$GITHUB_BASE_SHA" \
            --arg head_ref "$GITHUB_HEAD_REF" \
            --arg head_sha "$GITHUB_HEAD_SHA" \
        '
            .spec.template.spec.containers[0].args = $args.args
            | .spec.template.spec.containers[0].env = [
                {"name": "GITHUB_ACTION", "value": $action},
                {"name": "GITHUB_REPOSITORY", "value": $repository},
                {"name": "GITHUB_PR_NUMBER", "value": $pull_request},
                {"name": "GITHUB_BASE_REF", "value": $base_ref},
                {"name": "GITHUB_BASE_SHA", "value": $base_sha},
                {"name": "GITHUB_HEAD_REF", "value": $head_ref},
                {"name": "GITHUB_HEAD_SHA", "value": $head_sha}
            ]
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
                      securityContext:
                        fsGroup: 1000
                        fsGroupChangePolicy: OnRootMismatch
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
                        - name: crucible
                          mountPath: /home/linuxbrew/.local/share/buildkit
                        - name: secret
                          readOnly: true
                          mountPath: "/run/example/secret"
                        - name: docker
                          mountPath: /run/example/docker
                      volumes:
                      - name: crucible
                        persistentVolumeClaim:
                          claimName: crucible-nginx
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

# The build step receives the verified webhook fields as Job environment, not
# shell source. Branch names belong to contributors and must remain data even
# after the function body rides into the child pod.
function say_hello {
    eval `ssh-agent`
    typeset name=${GITHUB_REPOSITORY#*/*-}
    typeset image=harbor.example.org/example/${name}:pr-${GITHUB_PR_NUMBER}-${GITHUB_HEAD_SHA}
    millwright gitconfig
    millwright sshconfig
    millwright mirror --repository $GITHUB_REPOSITORY
    millwright merge --repository $GITHUB_REPOSITORY
    export BUILDKITD_FLAGS="--oci-worker-no-process-sandbox --root /home/linuxbrew/.local/share/buildkit"
    export ROOTLESSKIT="rootlesskit --copy-up=/run"
    export DOCKER_CONFIG=/run/example/docker
    export XDG_RUNTIME_DIR=/home/linuxbrew/.run
    mkdir -p $XDG_RUNTIME_DIR /home/linuxbrew/.local/share/buildkit && chmod 700 $XDG_RUNTIME_DIR
    buildctl-daemonless.sh build --frontend dockerfile.v0 --local context=/git/${name}/main --local dockerfile=/git/${name}/main --output type=image,name=${image},push=true
}

function {
    : ${GITHUB_REPOSITORY:?set GITHUB_REPOSITORY} ${GITHUB_PR_NUMBER:?set GITHUB_PR_NUMBER}
    : ${GITHUB_BASE_REF:?} ${GITHUB_BASE_SHA:?} ${GITHUB_HEAD_REF:?} ${GITHUB_HEAD_SHA:?}
    step -n build -- zsh_step -f say_hello
}

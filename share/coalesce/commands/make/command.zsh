function :help:make {
}

function :args:make {
    eval "$(args -bx h,help -- "$@")"
}

# The descriptor's metadata becomes the ConfigMap's metadata verbatim. Whatever
# it names — labels, annotations, anything Kubernetes accepts there — arrives
# untouched, so this command never has to learn what a pipeline label means.
# A descriptor with spec.template carries that pod template as data; one without
# it is a library. That is a structural distinction only, not an interpretation
# of what either artifact does. The descriptor rides inside the archive too,
# which costs nothing and leaves the artifact able to describe itself.
#
# The key is ball.tar.gz for every source, pipeline or library. This is the one
# renderer and it stamps the one name a pod template can mount and unpack — the
# loop is closed, so there is no second producer to accommodate.
#
# binaryData is printed rather than rendered, because a YAML writer is entitled
# to fold a long scalar and Kubernetes will not accept a folded base64 payload.
# The archive uses a sorted file list fed to a
# non-recursing tar, ustar to avoid extended headers, ownership flattened, and
# gzip told to omit its header, so an unchanged directory does not churn the
# ConfigMap. COPYFILE_DISABLE keeps macOS extended attributes from arriving as
# AppleDouble files.
function :execute:make {
    setopt localoptions pipefail
    typeset file separator= files=( "$@" )
    for file in "${(@)files}"; do
        [[ -e $file/coalesce.yaml ]] || abend 'not a coalesce directory'
    done
    for file in "${(@)files}"; do
        printf '%s' "$separator"
        printf -v separator '---\n'
        print -r -- 'apiVersion: v1'
        print -r -- 'kind: ConfigMap'
        print -r -- 'metadata:'
        gojq --yaml-input --yaml-output '.metadata' < $file/coalesce.yaml | sed 's/^/  /'
        if gojq --yaml-input -e '.spec.template != null' < $file/coalesce.yaml > /dev/null; then
            print -r -- 'data:'
            print -r -- '  template.yaml: |'
            gojq --yaml-input --yaml-output '.spec.template' < $file/coalesce.yaml | sed 's/^/    /'
        fi
        print -r -- 'binaryData:'
        print -nr -- '  ball.tar.gz: '
        (
            cd $file
            find . -type f -print | LC_ALL=C sort |
                COPYFILE_DISABLE=1 bsdtar -c --format ustar \
                    --uid 0 --gid 0 --uname root --gname root \
                    --no-recursion -T - -f - |
                gzip -n |
                base64 |
                tr -d '\n'
        )
        print
    done
}

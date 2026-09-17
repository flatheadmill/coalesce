#!/bin/zsh

# Run with zsh test/make.test.zsh; requires the same tools as coalesce make.
setopt errexit pipefail

typeset root=${0:A:h:h} tmp
tmp=$(mktemp -d)
{
    mkdir -p $tmp/first $tmp/second $tmp/extracted $tmp/staging $tmp/bin
    cp -Rp $root/test/hello $root/test/library $tmp/first/
    cp -Rp $root/test/library $root/test/hello $tmp/second/
    printf 'hidden source\n' > $tmp/first/hello/.hidden
    printf 'spaced name\n' > "$tmp/first/hello/a file"
    cp -p $tmp/first/hello/.hidden "$tmp/first/hello/a file" $tmp/second/hello/
    TZ=UTC find $tmp/first -type f -exec touch -t 200001010000.00 {} +
    TZ=UTC find $tmp/second -type f -exec touch -t 202601010000.00 {} +

    COPYFILE_DISABLE=1 bsdtar -c --format ustar -f $tmp/before.tar -C $tmp first second
    TMPDIR=$tmp/staging $root/bin/coalesce make $tmp/first/hello $tmp/first/library > $tmp/first.yaml
    TMPDIR=$tmp/staging $root/bin/coalesce make $tmp/second/hello $tmp/second/library > $tmp/second.yaml
    cmp $tmp/first.yaml $tmp/second.yaml
    COPYFILE_DISABLE=1 bsdtar -c --format ustar -f $tmp/after.tar -C $tmp first second
    cmp $tmp/before.tar $tmp/after.tar

    gojq --yaml-input -r 'select(.metadata.name == "hello") | .binaryData["ball.tar.gz"]' < $tmp/first.yaml |
        base64 -d | bsdtar -x -f - -C $tmp/extracted
    diff -r $tmp/first/hello $tmp/extracted
    [[ -x $tmp/extracted/run && ! -x $tmp/extracted/coalesce.yaml ]]
    [[ $(gojq --yaml-input -r 'select(.metadata.name == "hello") | .data["template.yaml"]' < $tmp/first.yaml) = \
        $(gojq --yaml-input --yaml-output '.spec.template' < $tmp/first/hello/coalesce.yaml) ]]
    gojq --yaml-input -e 'select(.metadata.name == "library") | .data == null' < $tmp/first.yaml > /dev/null

    printf '# changed content\n' >> $tmp/second/hello/run
    $root/bin/coalesce make $tmp/second/hello $tmp/second/library > $tmp/changed.yaml
    if cmp -s $tmp/first.yaml $tmp/changed.yaml; then
        print -u 2 'a content change must change the archive'
        false
    fi
    cp -p $tmp/first/hello/run $tmp/second/hello/run
    chmod -x $tmp/second/hello/run
    $root/bin/coalesce make $tmp/second/hello $tmp/second/library > $tmp/mode.yaml
    if cmp -s $tmp/first.yaml $tmp/mode.yaml; then
        print -u 2 'an executable-mode change must change the archive'
        false
    fi

    printf '#!/bin/sh\nexit 7\n' > $tmp/bin/bsdtar
    chmod +x $tmp/bin/bsdtar
    if PATH=$tmp/bin:$PATH TMPDIR=$tmp/staging $root/bin/coalesce make $tmp/first/hello > $tmp/failed.yaml 2> $tmp/failed.err; then
        print -u 2 'an archive failure must fail the command'
        false
    fi
    [[ $(<$tmp/failed.err) = *'unable to archive'* ]]
    typeset leftovers=( $tmp/staging/*(ND) )
    (( ! ${#leftovers} ))
    print 'coalesce make reproducibility checks passed'
} always {
    rm -rf $tmp
}

#!/usr/bin/env zsh

function _coalesce_cksum {
    typeset -i16 hex_var=$(printf '%s' ${1:-} | cksum | cut -d' ' -f1)
    print ${hex_var[4,-1]:l}
}

function _coalesce_init {
    typeset -gA _coalesce=( coalesce:queue '' coalesce:over 0 coalesce:parallel 1 )
    typeset -ga _coalesce_children=()
}

function step {
    eval "$(args n,name u,under p,parallel f,func -- "$@")"
    typeset node pod split=() under=() join=( coalesce ) queue=()
    if [[ -v o_under ]]; then
        join=( coalesce "${(As:/:)o_under}" )
    fi
    under="${(j:.:)join}"
    if [[ -v o_parallel ]]; then
        (( ${+_coalesce[${under}:queue]} )) || abend "no such node to put under"
        _coalesce[${under}.${o_name}:queue]=
        queue=( "${(@AQ)${(z)_coalesce[${under}:queue]}}" )
        queue+=( $o_name )
        _coalesce[${under}:queue]="${(@qq)queue}"
        _coalesce[${under}.${o_name}:kind]=tranche
        _coalesce[${under}.${o_name}:started]=0
        _coalesce[${under}.${o_name}:over]=0
        if (( ! o_parallel )); then
            _coalesce[${under}.${o_name}:parallel]=$((2**63 - 1))
        else
            _coalesce[${under}.${o_name}:parallel]=$o_parallel
        fi
    else
        (( ${+_coalesce[${under}:queue]} )) || abend "no such node to put under"
        typeset args=( "$@" )
        eval "$(args e,env -- "$@")"
        queue=( "${(@AQ)${(z)_coalesce[${under}:queue]}}" )
        queue+=( $o_name )
        _coalesce[${under}:queue]="${(@qq)queue}"
        _coalesce[${under}.${o_name}:kind]=pod
        _coalesce[${under}.${o_name}:args]=${(j: :)${(@qq)args}}
    fi
}

function :args:coalesce {
    eval "$(args -CU -bx h,help -s s,slug -- "$@")"
}

function :execute:coalesce {
    [[ -v o_slug ]] || abend 'slug is required'
    delegate "$@"
}

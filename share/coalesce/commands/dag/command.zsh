#!/usr/bin/env zsh

function _coalesce_dag_descend_json {
    typeset dag=${1:-} queue=() node
    shift
    integer parallel
    queue=( "${(@AQ)${(z)_coalesce[${dag}:queue]}}" )
    for node in "${(@)queue}"; do
        case $_coalesce[${dag}.${node}:kind] in
        (tranche)
            parallel=$(( _coalesce[${dag}.${node}:parallel] != 1 ))
            jo -- name=$node under=${dag} kind=tranche -b parallel=$parallel
            _coalesce_dag_descend_json ${dag}.${node}
            ;;
        (pod)
            jo -- name=$node under=${dag} kind=node
            ;;
        esac
    done
}

function _coalesce_dag_json {
    _coalesce_dag_descend_json coalesce
}

function :args:coalesce:dag {
    eval "$(args -bx h,help -- "$@")"
}

function :execute:coalesce:dag {
    _coalesce_init
    source $1
    _coalesce_dag_json | jq --slurp '
      . as $all |
      def find_children($path):
        $all[] |
        select(.under == $path) |
        . as $node |
        . + {
          children: [
            $all | find_children(
              if $path == null then $node.name
              else $path + "." + $node.name
              end
            )
          ]
        };

      [find_children("coalesce")]
    '
}

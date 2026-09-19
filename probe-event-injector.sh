#!/usr/bin/env bash
#################################################################
# Simple Script to inject events using probe event factory
# Author Avinash Krishnan
#
##################################################################

set -eo pipefail
SCRIPT_NAME=$(basename "$0")
CLEAR='\033[0m'
RED='\033[0;31m'

function usage() {

    cat <<-EOF
    $SCRIPT_NAME [options]

    -h | --help           this is some help text.
    -n | --namespace      this is my first option
    -v | --verbose        this is my second option
EOF
}

# parse params
while [[ "$#" -gt 0 ]]; do case $1 in
    -n | --namespace)
        NAMESPACE="$2"
        shift
        shift
        ;;
    -s | --context)
        CONTEXT="$2"
        shift
        shift
        ;;
    -v | --verbose)
        VERBOSE=1
        shift
        ;;

    *)
        usage
        shift
        shift
        ;;
    esac done

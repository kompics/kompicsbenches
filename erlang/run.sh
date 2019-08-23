#!/bin/bash

export RELX_REPLACE_OS_VARS=true
export NODE_NAME=$1
shift
exec _rel/erlang_benchmarks/bin/erlang_benchmarks console "$@"

#!/bin/bash
declare -a s=(
	"System: $(uname -a)" 
	"Git: branch: $(git rev-parse --abbrev-ref HEAD), commit: $(git rev-parse HEAD)"
	"Rust: $(rustc --version)"
	"Java: $(java --version)"
	"Erlang: $(erl -eval '{ok, Version} = file:read_file(filename:join([code:root_dir(), "releases", erlang:system_info(otp_release), "OTP_VERSION"])), io:fwrite(Version), halt().' -noshell)"
)
if [ -z "$1" ]
  then
    for val in "${s[@]}"; do
  		echo $val
	done
else
	file=$1/experimental_setup.out
	touch $file
    for val in "${s[@]}"; do
  		echo $val >> $file
	done
fi
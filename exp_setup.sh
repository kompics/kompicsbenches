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
	echo $'\n---------- Kompact Configurations ----------\n'
	for config_file in kompact/configs/*; do 
		echo $config_file;
		cat $config_file; 
	done
else
	file=$1/experimental_setup.out
	touch $file
    for val in "${s[@]}"; do
  		echo $val >> $file
	done
	echo $'\n---------- Kompact Configurations ----------\n' >> $file
	for config_file in kompact/configs/*; do 
		echo $config_file >> $file;
		cat $config_file >> $file; 
	done
fi
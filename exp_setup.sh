#!/bin/bash
file=$1/experimental_setup.out
touch $file
echo "System: $(eval "uname -a")" >> $file
echo "Git: branch: $(eval "git rev-parse --abbrev-ref HEAD"), hash: $(eval "git rev-parse HEAD")" >> $file
echo "Rust: $(eval "rustc --version")" >> $file
echo "Java: $(eval "java --version")" >> $file
echo "Erlang: $(eval "erl -eval '{ok, Version} = file:read_file(filename:join([code:root_dir(), \"releases\", erlang:system_info(otp_release), \"OTP_VERSION\"])), io:fwrite(Version), halt().' -noshell")" >> $file
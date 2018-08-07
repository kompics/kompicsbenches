#!/bin/bash
#nohup ./bench.sc client "$@" &> /dev/null &
nohup ./bench.sc client "$@" &> nohup.out &
echo $!

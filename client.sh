#!/bin/bash
#nohup ./bench.sc client "$@" &> /dev/null &
str="'$*'"
nohup ./bench.sc client "$@" &> nohup-"$str".out &
echo $!

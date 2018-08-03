#!/bin/bash
nohup ./bench.sc client "$@" &> /dev/null &
echo $!
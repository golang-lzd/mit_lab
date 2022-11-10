#!/bin/bash
cnt=1
while(( $cnt<=100 ))
do
    go test -run 2B
    let "cnt++"
done
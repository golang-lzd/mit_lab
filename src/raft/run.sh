#!/bin/bash
cnt=1
while(( $cnt<=100 ))
do
    go test -run Backup2B
    let "cnt++"
done
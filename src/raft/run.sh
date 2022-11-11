#!/bin/bash
cnt=1
test="A"
while(( $cnt<=20 ))
do
    go test -run 2${test} > run-${cnt}-1.log &
    go test -run 2${test}  > run-${cnt}-2.log&
    go test -run 2${test} > run-${cnt}-3.log&
    go test -run 2${test} > run-${cnt}-4.log&
    go test -run 2${test} > run-${cnt}-5.log&
    go test -run 2${test} > run-${cnt}-6.log&
    go test -run 2${test} > run-${cnt}-7.log&
    go test -run 2${test} > run-${cnt}-8.log&
    go test -run 2${test} > run-${cnt}-9.log&
    go test -run 2${test} > run-${cnt}-10.log&
    let "cnt++"
done
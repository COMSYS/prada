#!/bin/bash

PIDS=()
i="0"
for f in $(ls -1 .. | grep -E "^tweets2009-.*\.txt"); do
	bash prepare_inserts.sh ../$f $i > inserts_$f &
	i="$((${i} + 1))"
	PIDS=(${PIDS[*]} $!)
done
for pid in ${PIDS[*]}; do
	wait ${pid}
done

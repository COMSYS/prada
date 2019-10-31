#!/bin/bash
cat $1 | gawk -v p="$2" -v annots=0 -F $';' -f prepare_inserts.awk > 01_twis_cmds_0i.txt
p1=$!
cat $1 | gawk -v p="$2" -v annots=1 -F $';' -f prepare_inserts.awk > 01_twis_cmds_annot_0i.txt
p2=$!

#!/bin/bash

python precompute_mail.py
cp 01_mail_cmds_m1.txt 01_mail_cmds_annot_m1.txt
cp 01_mail_cmds_i1.txt 01_mail_cmds_annot_i1.txt
iconv -f utf-8 -t utf-8 -c 01_mail_cmds_0.txt -o 01_mail_cmds_0_fixed.txt
iconv -f utf-8 -t utf-8 -c 01_mail_cmds_annot_0.txt -o 01_mail_cmds_annot_0_fixed.txt
rm 01_mail_cmds_0.txt 01_mail_cmds_annot_0.txt
mv 01_mail_cmds_0_fixed.txt 01_mail_cmds_0.txt
mv 01_mail_cmds_annot_0_fixed.txt 01_mail_cmds_annot_0.txt

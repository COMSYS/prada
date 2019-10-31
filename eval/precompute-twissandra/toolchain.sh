#!/bin/bash

INPUT_FILE="${1}"
TWEETS_FILE="tweets_clean.txt"

bash clean_tweets.sh ${INPUT_FILE}
bash prepare_creates.sh
bash prepare_inserts.sh ${TWEETS_FILE}
bash tweets_per_user.sh ${TWEETS_FILE}
cat users_${TWEETS_FILE} | sort -rn > sorted_users.txt
head -n $(cat sorted_users.txt | grep -n -E "^ *${t} " | tail -n 1 | cut -f 1 -d":") sorted_users.txt > eligible_users.txt
python ./select_users.py
python ./prepare_timelines.py
bash prepare_userlines.sh
python prepare_userlines.py


bash glue_inserts.sh

cp 01_twis_cmds_t.txt 01_twis_cmds_annot_t.txt
cp 01_twis_cmds_u.txt 01_twis_cmds_annot_u.txt

FOLDER="$(date "+%Y-%m-%d")_twissandra"
mkdir -p $FOLDER
mv -t ${FOLDER} 01_twis_cmds_0.txt 01_twis_cmds_annot_0.txt 01_twis_cmds_t.txt 01_twis_cmds_annot_t.txt 01_twis_cmds_u.txt 01_twis_cmds_annot_u.txt
rm 01_twis_cmds_*.txt
rm eligible_users.txt selected_users.txt sorted_users.txt users_maxoffsets.txt users_tweets_clean.txt

bash check_dataset.sh ${FOLDER}

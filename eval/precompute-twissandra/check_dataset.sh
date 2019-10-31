#!/bin/bash

FOLDER=${1}
TWEETS="2000000"
USERS="1000"
TWEETS_PER_PAGE="50"

EXIT=0

function assert {
    if [ "x${1}x" == "x${2}x" ]; then
        echo -e "\033[32mSuccess\e[0m"
        EXIT=$((${EXIT} | 0))
    else
        echo -e "\033[31mFAILURE\e[0m"
        EXIT=$((${EXIT} | 1))
    fi
}

printf "Correct escaping: "
r="$(grep -o -E "(^'[^']|[^']'[^']|[^']'$)" tweets_clean.txt | wc -l)"
assert ${r} 0

echo "Number INSERTs tweets: "
printf "    vanilla: "
r="$(fgrep "INSERT INTO twissandra.tweets" ${FOLDER}/01_twis_cmds_0.txt | wc -l)"
assert ${r} ${TWEETS}
printf "    comsys:  "
r="$(fgrep "INSERT INTO twissandra.tweets" ${FOLDER}/01_twis_cmds_annot_0.txt | wc -l)"
assert ${r} ${TWEETS}

echo "Number INSERTs userline: "
printf "    vanilla: "
r="$(fgrep "INSERT INTO twissandra.userline" ${FOLDER}/01_twis_cmds_0.txt | wc -l )"
assert ${r} $(( $USERS * $TWEETS_PER_PAGE ))
printf "    comsys:  "
r="$(fgrep "INSERT INTO twissandra.userline" ${FOLDER}/01_twis_cmds_annot_0.txt | wc -l )"
assert ${r} $(( $USERS * $TWEETS_PER_PAGE ))

echo "Number INSERTs timeline: "
printf "    vanilla: "
r="$(fgrep "INSERT INTO twissandra.timeline" ${FOLDER}/01_twis_cmds_0.txt | wc -l )"
assert ${r} $(( $USERS * $TWEETS_PER_PAGE ))
printf "    comsys:  "
r="$(fgrep "INSERT INTO twissandra.timeline" ${FOLDER}/01_twis_cmds_annot_0.txt | wc -l )"
assert ${r} $(( $USERS * $TWEETS_PER_PAGE ))


echo "Number SELECTs userline: "
printf "    vanilla: "
r="$(fgrep "twissandra.userline" ${FOLDER}/01_twis_cmds_u.txt | wc -l )"
assert ${r} $USERS
printf "    vanilla: "
r="$(fgrep "twissandra.tweets" ${FOLDER}/01_twis_cmds_u.txt | wc -l )"
assert ${r} $USERS
printf "    comsys:  "
r="$(fgrep "twissandra.userline" ${FOLDER}/01_twis_cmds_annot_u.txt | wc -l )"
assert ${r} $USERS
printf "    comsys:  "
r="$(fgrep "twissandra.tweets" ${FOLDER}/01_twis_cmds_annot_u.txt | wc -l )"
assert ${r} $USERS

echo "Number SELECTs timeline: "
printf "    vanilla: "
r="$(fgrep "twissandra.timeline" ${FOLDER}/01_twis_cmds_t.txt | wc -l )"
assert ${r} $USERS
printf "    vanilla: "
r="$(fgrep "twissandra.tweets" ${FOLDER}/01_twis_cmds_t.txt | wc -l )"
assert ${r} $USERS
printf "    comsys:  "
r="$(fgrep "twissandra.timeline" ${FOLDER}/01_twis_cmds_annot_t.txt | wc -l )"
assert ${r} $USERS
printf "    comsys:  "
r="$(fgrep "twissandra.tweets" ${FOLDER}/01_twis_cmds_annot_t.txt | wc -l )"
assert ${r} $USERS

if [ "x${EXIT}x" == "x0x" ]; then
    echo -e "\033[32mEverything went well\e[0m"
else
    echo -e "\033[31mAn error occurred!\e[0m"
fi

exit ${EXIT}

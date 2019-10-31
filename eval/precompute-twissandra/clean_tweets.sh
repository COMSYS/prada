#!/bin/bash
cat $1 | mawk -v p="$2" -F $'\t' -f clean_tweets.awk > tweets_clean.txt

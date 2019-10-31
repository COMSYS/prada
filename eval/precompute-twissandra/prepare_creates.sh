#!/bin/bash

echo "CREATE KEYSPACE twissandra WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};" > 01_twis_cmds_0c.txt
echo "CREATE TABLE twissandra.tweets (tweet_id text PRIMARY KEY, username text, body text);" >> 01_twis_cmds_0c.txt
echo "CREATE TABLE twissandra.userline (username text, time text, tweet_id text, PRIMARY KEY (username, time)) WITH CLUSTERING ORDER BY (time DESC);" >> 01_twis_cmds_0c.txt
echo "CREATE TABLE twissandra.timeline (username text, time text, tweet_id text, PRIMARY KEY (username, time)) WITH CLUSTERING ORDER BY (time DESC);" >> 01_twis_cmds_0c.txt

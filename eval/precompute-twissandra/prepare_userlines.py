#!/usr/bin/env python

import sys

from random import Random
from os import popen

page_limit = 50
if len(sys.argv) == 2 and sys.argv[1] == 'test':
    page_limit = 30

offsetRandom = Random()
offsetRandom.seed()
orderRandom = Random()
orderRandom.seed()

tweets_filename = "tweets_clean.txt"
with open('users_maxoffsets.txt', 'r') as f:
  user_strs = f.readlines()
users = []
user_uuids = {}
for user_str in user_strs:
  tmp = user_str.split(';')
  username = tmp[0]
  number_tweets = int(tmp[1], 10)
  offset = offsetRandom.randrange(0, (number_tweets - page_limit) + 1) # Ensure that we always fetch a full page despite randomization
  users.append((username, offset))

with open('01_twis_cmds_0u.txt', 'w') as fout_inserts:
    c = 0
    for user in users:
        userline_uuids = []
        user_tweets = popen('fgrep -m ' + str(user[1] + page_limit) + ' "' + user[0] + ';00000000-0000-" ' + tweets_filename).readlines()
        user_tweets = user_tweets[user[1] : (user[1] + page_limit)]
        for tweet in user_tweets:
            uuid = tweet.split(';')[1]
            fout_inserts.write('INSERT INTO twissandra.userline (username, time, tweet_id) VALUES (\'%s\', \'%s\', \'%s\');\n' % (user[0], uuid, uuid))
            userline_uuids.append(uuid)
        user_uuids[user[0]] = userline_uuids
        sys.stdout.write('.')
        sys.stdout.flush()
        if c % 50 == 49:
            sys.stdout.write('\n')
            sys.stdout.flush()
        c = (c + 1) % 50

with open('01_twis_cmds_u.txt', 'w') as fout_selects:
    indices = [i for i in xrange(len(user_uuids.keys()))]
    orderRandom.shuffle(indices)
    usernames = user_uuids.keys()
    for i in indices:
        user = usernames[i]
        uuid_list = ', '.join(map(lambda x: '\'' + x + '\'', user_uuids[user]))
        fout_selects.write('SELECT time, tweet_id FROM twissandra.userline WHERE username = \'%s\' LIMIT %d;\n' % (user, page_limit))
        fout_selects.write('SELECT username, body FROM twissandra.tweets WHERE tweet_id IN (%s);\n' % (uuid_list))

#!/usr/bin/env python

from os import popen
from random import Random
import sys

number_users = 1000 # 1000 users provide their data; 100 of them are queried
window_size = 100000
tweets_per_page = 50
filename = "eligible_users.txt"
total_users = int(popen('wc -l ' + filename + ' | cut -f 1 -d" "').readline())
number_tweets = 2000000

seeder = Seeds()
userSelectRandom = Random() # Select users that are considered (>= 50 tweets)
userSelectRandom.seed()
tweetOffsetRandom = Random() # Select tweet offset for their timeline tweets; [0, numberTweets - windowSize)
tweetOffsetRandom.seed()
tweetChoiceRandom = Random() # Select tweets from [tweetOffset, tweetOffset + windowSize)
tweetChoiceRandom.seed()

userids = userSelectRandom.sample(xrange(total_users), number_users)
with open(filename, 'r') as f:
  users = f.readlines()
users = [ users[i] for i in userids ]
users = map(lambda x: x.split(' ')[-1].rstrip(), users)

relative_window = xrange(window_size)
with open('selected_users.txt', 'w') as f:
  for user in users:
    offset = tweetOffsetRandom.randrange(0, number_tweets - window_size)
    relative_tweet_ids = sorted(tweetChoiceRandom.sample(relative_window, tweets_per_page))
    tweet_ids_serial = ','.join(map(lambda x: str(x), relative_tweet_ids))
    f.write('%s;%d;%s\n' % (user, offset, tweet_ids_serial))

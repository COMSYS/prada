#!/usr/bin/env python

import random

page_limit = 50

with open('selected_users.txt', 'r') as f:
  user_strs = f.readlines()
users = []
user_uuids = {}
for user_str in user_strs:
  tmp = user_str.split(';')
  username = tmp[0]
  offset = int(tmp[1], 10)
  tweet_ids = tmp[2].split(',')
  tweet_ids = map(lambda x: offset + int(x, 10), tweet_ids)
  users.append((username, tweet_ids))
users = [ [ (user[0], user[1][i]) for i in xrange(len(user[1])) ] for user in users ]
conv_tweet_ids = reduce(lambda x, u: x + u, users, [])
sorted_conv_tweet_ids = sorted(conv_tweet_ids, key = lambda x: x[1])

chunksize = 512 * 1024 * 1024 # Read 512 MiB at once
small_chunksize = 512 * 1024 * 1024

fout_inserts = open('01_twis_cmds_0t.txt', 'w')
with open('tweets_clean.txt', 'r') as f:
  consumed_breaks = 0
  break_ctr = 0
  chunk_offset = 0
  old_offset = 0
  chunk = f.read(chunksize)
  nbreaks = chunk.count('\n')
  while len(sorted_conv_tweet_ids) > 0:
    next_user = sorted_conv_tweet_ids[0][0]
    next_tweet_id = sorted_conv_tweet_ids[0][1]
    sorted_conv_tweet_ids = sorted_conv_tweet_ids[1:]
    # Read new chunks until we know the next tweet starts in it
    while break_ctr + nbreaks < next_tweet_id:
      print('Reading new chunk.')
      consumed_breaks = break_ctr
      chunk = f.read(chunksize)
      if len(chunk) == 0:
        raise RuntimeError('Not all tweets found!')
      break_ctr += nbreaks
      chunk_offset = 0
      old_offset = 0
    # If the number of seen linebreaks equals the desired tweet offset, the tweet is only partially contained
    # -> Append smaller chunks until the whole tweet is contained
    # Note: For tweets this should always hold after reading another chunk of > 194 bytes (username;uuid;tweet)
    if break_ctr + nbreaks == next_tweet_id:
      smallchunk = ''
      small_nbreaks = 0
      while small_nbreaks == 0:
        new_smallchunk = f.read(small_chunksize)
        if len(new_smallchunk) == 0:
          raise RuntimeError('Not all tweets found!')
        new_nbreaks = new_smallchunk.count('\n')
        smallchunk += new_smallchunk
        small_nbreaks += new_nbreaks
      chunk += smallchunk
      break_ctr += small_nbreaks
    # Now we are sure that the whole tweet we need to read is contained in our (possibly extended) chunk.
    # -> Search from the last position onward
    for i in xrange(next_tweet_id - consumed_breaks + 1):
      old_offset = chunk_offset + 1
      chunk_offset = chunk.find('\n', old_offset)
      consumed_breaks += 1
    line = chunk[old_offset:chunk_offset]
    data = line.split(';')
    try:
      uuid = data[1]
      if not next_user in user_uuids.keys():
        user_uuids[next_user] = [ uuid ]
      else:
        user_uuids[next_user].append(uuid)
    except:
      print('Could not process line:\n%s' % (line))
    fout_inserts.write('INSERT INTO twissandra.timeline (username, time, tweet_id) VALUES (\'%s\', \'%s\', \'%s\');\n' % (next_user, uuid, uuid))
fout_inserts.close()

random.seed()
with open('01_twis_cmds_t.txt', 'w') as fout_selects:
    indices = [i for i in xrange(len(users))]
    random.shuffle(indices)
    usernames = user_uuids.keys()
    for i in indices:
        user = usernames[i]
        uuid_list = ', '.join(map(lambda x: '\'' + x + '\'', user_uuids[user]))
        fout_selects.write('SELECT time, tweet_id FROM twissandra.timeline WHERE username = \'%s\' LIMIT %d;\n' % (user, page_limit))
        fout_selects.write('SELECT username, body FROM twissandra.tweets WHERE tweet_id IN (%s);\n' % (uuid_list))


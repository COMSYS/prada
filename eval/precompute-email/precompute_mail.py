#!/usr/bin/env python

from os import walk
from random import Random
from uuid import UUID
import sys
import os


# Path to enron dataset, default assumes "~/.prada/email"
mailFolder = '~/.prada/email'

limitOverride = 30000 # Largest mailbox hosts 28465 mails; if not set explicitly, cassandra uses 10000 instead
selectCount = 10000
cqlFolder = 'eval-email'
cqlFile = "{}/01_mail_cmds_{}.txt".format(cqlFolder)
rounds = 1
userCount = 50
mailsPerUser = 100

maxPayload = 65535 # Truncation required for single mails by Cassandra
maxMail = -1

class Mail:
  user = ""
  header = ""
  body = ""
  id = ""
  mailbox = ""


''' Annotation keys:
    UE: us-east
    UC: us-central
    UW: us-west
    CE: canada-east
    US: us-southcentral
    EW: eu-west
    AE: asia-east
    AS: asia-southeast
    JE: japan-east
    EN: eu-north '''
annotations = ['UE', 'UC', 'UW', 'CE', 'US', 'EW', 'AE', 'AS', 'JE', 'EN'] # Cloud distribution

if not os.path.exists(cqlFolder):
    os.makedirs(cqlFolder)

mailFiles = []
for root, dirs, files in walk(mailFolder):
  for f in files:
    mailFiles.append((root + '/' + f))

mails = []
mailboxes = set()
userAnnotations = {}
annotationRandom = Random()
annotationRandom.seed()
oldUser=""
uc = 0
mc = 0
skip = False
print("Reading mails...")
for i in xrange(len(mailFiles)):
  m = Mail()
  file = mailFiles[i]
  m.user = file.split('/')[1]
  m.id = UUID(int=(i + 1))
  m.mailbox = UUID(int=int(m.user.encode("hex"), 16))
  if not m.user == oldUser:
    uc = uc + 1
    print("User: {} ({})".format(m.user, uc))
    if m.mailbox in mailboxes:
        print('Collision for user ' + m.user)
    mailboxes.add(m.mailbox)
    userAnnotations[m.user] = annotationRandom.randrange(0, len(annotations))
    oldUser = m.user
    skip = False
  elif skip:
    continue
  # Prevent inserting ALL the mails
  mc = mc + 1
  if maxMail != -1 and mc > maxMail:
    break
  header = []
  body = []
  headerComplete = False
  with open(file, 'r') as f:
    for l in f:
      l = l.rstrip('\n').rstrip('\r').replace("'", "''")
      if headerComplete:
        body.append(l)
      elif l == '':
        headerComplete = True
      else:
        header.append(l)
  m.body = " ".join(body)
  m.header = " ".join(header)
  mails.append(m)

print(str(len(mails)))


print("")
print("Writing output files...")
print("CREATE")
# CREATEs, INSERTs
for s in ["", "annot_"]:
  with open(cqlFile.format("{}0").format(s), 'w') as f:
    # CREATE
    f.write("CREATE KEYSPACE mail WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};\n")
    f.write(
      'CREATE TABLE mail.mailboxes ("mailboxid" text, "mailid" text, "header" text, PRIMARY KEY(mailboxid, mailid));\n')
    f.write('CREATE TABLE mail.mails ("mailid" text PRIMARY KEY, "header" text, "body" text);\n')
    # INSERT
    for m in mails:
      query_length = len("INSERT INTO mail.mails (mailid, header, body) VALUES ('', '', '') WITH ANNOTATIONS 'country' = { 'XX' };\n") + 36 + len(m.header[:maxPayload]) + len(m.body[:maxPayload])
      overcut = query_length - maxPayload
      maxlen_body = len(m.body[:maxPayload]) - (overcut if len(m.body[:maxPayload]) > overcut else 0)
      maxlen_header = len(m.header[:maxPayload]) - (overcut if len(m.header[:maxPayload]) > overcut and maxlen_body == len(m.body[:maxPayload])  else 0)
      if s == "":
        f.write("INSERT INTO mail.mailboxes (mailboxid, mailid, header) VALUES ('{}', '{}', '{}');\n"
                .format(m.mailbox, m.id, m.header[:maxlen_header]))
        f.write("INSERT INTO mail.mails (mailid, header, body) VALUES ('{}', '{}', '{}');\n"
                .format(m.id, m.header[:maxlen_header], m.body[:maxlen_body]))
      else:
        f.write("INSERT INTO mail.mailboxes (mailboxid, mailid, header) VALUES ('{}', '{}', '{}') {};\n"
                .format(m.mailbox, m.id, m.header[:maxlen_header], 'WITH ANNOTATIONS \'country\' = { \'' + annotations[userAnnotations[m.user]] + '\' }'))
        f.write("INSERT INTO mail.mails (mailid, header, body) VALUES ('{}', '{}', '{}') {};\n"
                .format(m.id, m.header[:maxlen_header], m.body[:maxlen_body], 'WITH ANNOTATIONS \'country\' = { \'' + annotations[userAnnotations[m.user]] + '\' }'))

print("SELECT mailboxes")
# SELECTs mailboxes
for r in xrange(rounds):
  with open(cqlFile.format("i{}".format(r+1)), 'w') as f:
    for mailbox in mailboxes:
      f.write("SELECT * FROM mail.mailboxes WHERE mailboxid = '{}' LIMIT {};\n".format(mailbox, limitOverride))

print("SELECT mails")
# SELECTs mails
for r in xrange(rounds):
  rnd = Random()
  rnd.seed()
  rnd.shuffle(mails)
  i = 0
  with open(cqlFile.format("m{}".format(r+1)), 'w') as f:
    for mail in mails:
      if i < selectCount:
        f.write("SELECT * FROM mail.mails WHERE mailid = '{}' LIMIT {};\n".format(mail.id, limitOverride))
        i = i + 1
      else:
        break

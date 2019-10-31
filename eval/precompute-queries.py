#!/usr/bin/env python2.7
""" Create CQL queries for microbenchmarking evaluation.

    Usage:
        ./precompute-queries.py runId replF fieldLength [updates|load|traffic]

    Options:
        - runId  -  Create independent queries for multiple evaluation runs
        - replF  -  Replication factor for the keyspace created
        - fieldLength  -  lengths of each (of ten) payload field
        - updates  -  Skip SELECTs and DELETEs
        - load  -  Only perform INSERTs (e.g., pre-load cluster data)
        - traffic  -  Extended measurement duration for traffic measurements

    Outputs command files to:
        ${runId}_eval_cmds[_annot]_(0|c|r|u|d).txt
"""

import random
from sys import argv
import string

annotations = []

long_traffic = True
fieldLength = int(argv[3])

only_updates = False
load_measurement = False
traffic_measurement = False
if len(argv) == 5 and argv[4] == 'updates':
    only_updates = True
if len(argv) == 5 and argv[4] == 'load':
    load_measurement = True
if len(argv) == 5 and argv[4] == 'traffic':
    traffic_measurement = True

lt_measurement = (load_measurement or traffic_measurement)

nTests = 50 if not lt_measurement else 0
nMeasure = 1000 if not long_traffic else 2000
nError = 50 if not lt_measurement else 0

if not lt_measurement:
    random.seed(0)

run = int(argv[1]) - 1

print('nTests:   ' + str(nTests))
print('nMeasure: ' + str(nMeasure))
print('nError:   ' + str(nError))
print('Run:      ' + str(run))


def getAnnot():
    return ' ' + annotations[random.randrange(len(annotations))]


def getField(update=False):
    prefix = 'update' if update else ''
    length = fieldLength - len(prefix)
    return '\'' + prefix + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length)) + '\''


def getKeyID(i, run, effective):
    return ('{0:1d}'.format(run) if effective else '0') + '{0:013d}'.format(i)


number_countries = 10
annotations = list()
for c1 in xrange(0, number_countries):
    if c1 + 1 == number_countries:
        continue
    for c2 in xrange(c1 + 1, number_countries):
        if c2 + 1 == number_countries:
            continue
        for c3 in xrange(c2 + 1, number_countries):
            annotations.append('WITH ANNOTATIONS \'country\' = { \'c' + str(c1) + '\', \'c' + str(c2) + '\', \'c' + str(c3) + '\' }')
            print(annotations[-1])

for o in ['0', 'c', 'r', 'u', 'd']:
    for m in xrange(2):
        if m == 0:
            path = argv[1] + '_eval_cmds_' + o + '.txt'
        else:
            path = argv[1] + '_eval_cmds_annot_' + o + '.txt'

        with open(path, 'w') as f:
            # Create (keyspace)
            if o == '0':
                f.write('CREATE KEYSPACE eval WITH replication = {\'class\':\'SimpleStrategy\', \'replication_factor\':' + argv[2] + '};\ncreate table eval.usertable (y_id varchar primary key,field0 varchar,field1 varchar,field2 varchar,field3 varchar,field4 varchar,field5 varchar,field6 varchar,field7 varchar,field8 varchar,field9 varchar) WITH read_repair_chance = 0.1;\n')

            # Inserts (C)
            if o == 'c':
                for i in xrange(nTests):
                    annot = getAnnot() if m == 1 else '';
                    f.write('INSERT INTO eval.usertable (y_id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (\'trn{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\', ' + ', '.join([getField() for i in xrange(10)]) + ')' + annot + ';\n')
                for i in xrange(nMeasure + nError):
                    annot = getAnnot() if m == 1 else '';
                    f.write('INSERT INTO eval.usertable (y_id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (\'run{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\', ' + ', '.join([getField() for i in xrange(10)]) + ')' + annot + ';\n')

            # Selects
            if o == 'r' and (not only_updates and not load_measurement):
                for i in xrange(nTests):
                    f.write('SELECT * FROM eval.usertable WHERE y_id = \'trn{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\';\n')
                for i in xrange(nMeasure + nError):
                    f.write('SELECT * FROM eval.usertable WHERE y_id = \'run{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\';\n')

            # Updates
            if o == 'u' and not load_measurement:
                for i in xrange(nTests):
                    annot = getAnnot() if m == 1 else '';
                    f.write('INSERT INTO eval.usertable (y_id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (\'trn{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\', ' + ', '.join([getField(True)] + [getField() for i in xrange(9)]) + ')' + annot + ';\n')
                for i in xrange(nMeasure + nError):
                    annot = getAnnot() if m == 1 else '';
                    f.write('INSERT INTO eval.usertable (y_id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (\'run{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\', ' + ', '.join([getField(True)] + [getField() for i in xrange(9)]) + ')' + annot + ';\n')

            # Deletes
            if o == 'd' and (not only_updates and not load_measurement):
                for i in xrange(nTests):
                    f.write('DELETE FROM eval.usertable WHERE y_id = \'trn{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\';\n')
                for i in xrange(nMeasure + nError):
                    f.write('DELETE FROM eval.usertable WHERE y_id = \'run{}id'.format(run) + getKeyID(i, run, lt_measurement) + '\';\n')

#!/usr/bin/env python

from random import Random
from uuid import UUID
import glob
import csv
import datetime
import string

# Specify path where your input data resides, default assumes "~/.prada/dweet/"
path = '~/.prada/dweet'
# Specify the number of measurements you want to cover
max_measurements = 1000

output_files = {
'baseline_setup': '01_dweet_cmds_0.txt',
'prada_setup': '01_dweet_cmds_annot_0.txt',

'all_sensorlist': '01_dweet_cmds_s1.txt',
'all_payload': '01_dweet_cmds_p1.txt',
}

dweet_data = {
	'sizes' : sorted(glob.glob(path + '*-messagesizes.csv')),
	'frequencies': sorted(glob.glob(path + '*-messagefrequency.csv')),
}

limitOverride = 300000 # Create sane custom limit for measurements
number_payload = 10000

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
available_annotations = ['UE', 'UC', 'UW', 'CE', 'US', 'EW', 'AE', 'AS', 'JE', 'EN'] # Cloud distribution

### CONFIGURATION ENDS

seeder = Seeds()
annotationRandom = Random()
annotationRandom.seed()
payloadRandom = Random()
payloadRandom.seed()
shuffleRandom = Random()
shuffleRandom.seed()

def get_payload(size):
	return ''.join(payloadRandom.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(size))

sizes = {}
annotations = {}
counts = {}

# first read sizes for all seen sensors and assign policies per sensor
for dweet_size_data in dweet_data['sizes']:
		
	with open(dweet_size_data, 'rb') as csvfile:
		reader = csv.reader(csvfile, delimiter=';')
		for row in reader:
			sizes[row[0]] = int(row[1])
			annotations[row[0]] = annotationRandom.choice(available_annotations)
			counts[row[0]] = 0
		
# then read the frequency of sensor measurements
count_measurements = 0
with open(output_files['baseline_setup'], 'w') as f_baseline:
	with open(output_files['prada_setup'], 'w') as f_prada:
		for f in [f_prada, f_baseline]:
			f.write("CREATE KEYSPACE dweet WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};\n")
			
			# https://www.datastax.com/dev/blog/we-shall-have-order
			f.write("CREATE TABLE dweet.overview (sensorname text, time text, id text, PRIMARY KEY ((sensorname), time)) WITH CLUSTERING ORDER BY (time DESC);\n");
			f.write("CREATE TABLE dweet.data (id text PRIMARY KEY, sensorname text, time text, payload text);\n");

		for dweet_frequency_data in dweet_data['frequencies']:
			with open(dweet_frequency_data, 'rb') as csvfile:
				reader = csv.reader(csvfile, delimiter=';')
				for row in reader:
					if count_measurements >= max_measurements:
						break
					count_measurements += 1
					measurement_id = UUID(int=count_measurements)
					sensor = row[0]
					try:
						time = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S.%f')
					except ValueError:
						time = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
			
					if sizes[sensor] > 65535:
						print 'WARNING: Payload too large?'
			
					counts[sensor] += 1
			
					overview_query = "INSERT INTO dweet.overview (sensorname, time, id) VALUES ('{}', '{}', '{}')".format(sensor, time, measurement_id)
					f_baseline.write(overview_query + ";\n")
					f_prada.write(overview_query + " WITH ANNOTATIONS 'country' = {'" + annotations[sensor] + "'};\n")
					
					data_query = "INSERT INTO dweet.data (id, sensorname, time, payload) VALUES ('{}', '{}', '{}', '{}')".format(measurement_id, sensor, time, get_payload(sizes[sensor]))
					f_baseline.write(data_query + ";\n")
					f_prada.write(data_query + " WITH ANNOTATIONS 'country' = {'" + annotations[sensor] + "'};\n")

stats_count_sensors = 0
stats_min_size = 65535
stats_max_size = 0


# create SELECT queries for sensorlist (overview of all data for a given sensor):
with open(output_files['all_sensorlist'], 'w') as f:
	for sensor in sizes:
		if counts[sensor] > 0:
			f.write("SELECT sensorname, time, id FROM dweet.overview WHERE sensorname = '{}' LIMIT {};\n".format(sensor, limitOverride))

			# calculate some statistics
			stats_count_sensors += 1
			if sizes[sensor] < stats_min_size:
				stats_min_size = sizes[sensor]
			if sizes[sensor] > stats_max_size:
				stats_max_size = sizes[sensor]

print "data set contains {} IoT messages of size {} B to {} B from {} different devices".format(max_measurements, stats_min_size, stats_max_size, stats_count_sensors)

# create SELECT queries for $number_payload random sensor measurements
print "Count_measurements: ", count_measurements
with open(output_files['all_payload'], 'w') as f:
	for i in shuffleRandom.sample(range(1, count_measurements + 1), number_payload):
		measurement_id = UUID(int=i)
		f.write("SELECT id, sensorname, time, payload FROM dweet.data WHERE id = '{}';\n".format(measurement_id))



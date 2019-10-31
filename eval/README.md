# PRADA Evaluation

## Overview

Measuring performance metrics of PRADA consists of the following essential steps:

1. Preparing commands setting up the Cassandra column families, inserting the prepared input data, and querying it
2. Conducting the desired measurements
3. Evaluating the results

In the following, we provide pointers for starting your own measurements based on the use microbenchmark and use-case measurements we conducted.
Finally, we outline how to prepare data according to our use cases.

### Command Preparation

We instrument PRADA by creating CQL files containing batches of either preparation commands or actual queries to be measured.
This folder contains preparation scripts to create these batches of CQL queries in separate text files, following the naming convention `[run]_[case]_cmds_[_annot]_[op].txt`, allowing separating query batches for (a) different use cases, (b) required phases within a use case, e.g., to separate precomputation phases from measured querues, and (c) redundant evaluation runs.

We provide the following preparation scripts for synthetic data and our [use cases](#preparing-use-case-input-data), respectively:

* `eval/precompute-queries.py`: Create queries over synthetic data for microbenchmarking PRADA's CRUD (Create, Read, Update, Delete) operations.
* `eval/precompute-email`: Use `precompute_mail.sh` to process the [Enron email dataset](#enron-mail-dataset) directly.
* `eval/precompute-twissandra`: We provide several scripts, processing the [Twitter7](#twitter7-dataset) data in multiple steps.
  `toolchain.sh` accumulates all conducted steps.
* `eval/precompute-dweet`: `precompute_dweet.py` processes our [IoT data](#iot-data).

### Conducting Measurements

To conduct measurements, after [preparing the CQL files](#command-preparation) boot your PRADA cluster.
Once booted successfully, feed the CQL files to the cluster with Cassandra's `cqlsh` tool and save the logs, e.g.:

```
cd prada-src/bin
cqlsh -f 01_eval_cmds_r.txt &> ~/.prada/log/01_eval_cmds_r.log
```

After the measurement is concluded, i.e., the last `cqlsh` call terminated, you can shutdown the cluster using `bin/prada stop` on all nodes, and [analyze the resulting log files](#evaluating-measurement-results).

#### Traffic Measurements

We use [`tcpstat`](https://frenchfries.net/paul/tcpstat/) to collect statistics on outgoing traffic on each cluster node.
For that purpose, we provide the script `bin/traffic` (uses `sudo` internally).

Start traffic monitoring before processing the CQL queries to be measured:
```
cd bin
./traffic eth0 start  # assuming eth0 is the correct network interface
```

After `cqlsh` concluded, stop monitoring traffic (sanely shutting down `tcpstat`):
```
./traffic stop
cd ..
```

The respective log is, by default, written to `~/.prada/log/traffic.txt`.
However, you can change the output file by adapting `$OUTFILE` in `bin/traffic`.

#### Storage Measurements

To measure the load across a PRADA cluster, we make use of Cassandra's `nodetool`.

After performing all Create opeations, first flush all tables from the memtable:
```
cd prada-src/bin
./nodetool flush
```

Then, we can use the `cfstats` nodetool (now named [`tablestats`](https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/tools/toolsTablestats.html)) to get statistics about a certain column family, e.g. `eval.usertable` as created through our `precompute-queries.py` script:
```
./nodetool cfstats \
    eval.usertable \
    eval_references.usertable \
    eval_redirecteddata.usertable \
    dataannotation.country \
    dataannotation.encryption \
    dataannotation.ramdisk
```
Here, it is important to adjust the considered column families to your setup, especially `datannotation.*` if you set up a different `annotation.conf`.

Specifically, we are interested in the following information provided by `cfstats`:
```
./nodetool cfstats eval.usertable [...] | grep -E "(Keyspace:|Space used|Table:) > ~/.prada/log/01_eval_cmds_c.lod
```
This command provides a simple means of getting only the occupied space by each table in each keyspace of your cluster.

### Evaluating Measurement Results

Following the above-mentioned evaluation methods, you can gain data on (a) query completion time, (b) outgoing traffic, and (c) load distribution per cluster node.
We now outline how to extract this data systematically from Cassandra's logs and the extracted statistics.

#### Query Completion Time

To track information about query completion time make sure to set `evalMode = true;` and keep `details = false;` in `prada-src/src/java/org/apache/cassandra/annotation/Evaluation.java`.

In this state, PRADA will create a file `./eval/eval-measurements-1.txt` containing the queries executed alongside with their query completion time in microseconds, e.g.:
```
QueryProcessor.process(String, QueryState, QueryOptions)
INSERT INTO eval.usertable (y_id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES ('userid40000000000000', '566Z0FSJG7IIDR9UTIZF', 'TLEF7BCN0N6BLOZDEQ8Y', 'I21P1LP3NK0MP6GSKAYI', 'G9AW5FUMXKA14Z7B3UML', 'QZAEVQKPY9HHMOV0FJ59', 'ON2XKJX32KR561BC6JLA', '4CN5TT5DFY502LSJOP0L', 'A28TWSM2A8K4PP7X5CAX', 'A1IUNHXINQY0DK7YMBAJ', 'DPX5M4FD6CVBJSBJE3JK') WITH ANNOTATIONS 'country' = { 'DE', 'FI', 'IT' };
TIME: 23274us
```

Subsequently, you can hence `grep` for the query types you are interested in and preserve the accompanying query completion time as follows:
```
query_pattern="INSERT INTO eval.usertable"
cat eval/eval-measurements-1.txt | grep -A 1 ${query_pattern} | grep -o -E "^(${query_pattern}|TIME: [0-9]+us)"
```
However, since queries can be executed through different threads and `eval-measurement-1.txt` does not guarantee consistent thread ordering, you cannot iterate over queries one by one assuming a correct ordering.

#### Outgoing Traffic

Our [`traffic` tool](#traffic-measurements) uses `tcpstat` to accumutlate outgoing traffic in (by default) `~/.prada/log/traffic.txt`.
This file has a very simple layout to parse directly:
```
b=[number_bytes_sent]
p=[numper_packets_sent]
```

#### Cluster Load Distribution

By applying the filtering described [before](#storage-measurements), it already becomes easy to extract the relevant data per keyspace and column family as the generated `*.lod` files have the following format:
```
Keyspace: eval_redirecteddata
                Table: usertable
                Space used (live), bytes: 92887
                Space used (total), bytes: 92887
Keyspace: eval_references
                Table: usertable
                Space used (live), bytes: 12079
                Space used (total), bytes: 12079
Keyspace: dataannotation
                Table: ramdisk
                Space used (live), bytes: 10017
                Space used (total), bytes: 10017
                Table: country
                Space used (live), bytes: 10037
                Space used (total), bytes: 10037
                Table: encryption
                Space used (live), bytes: 10256
                Space used (total), bytes: 10256
```

Due to using `nodetool flush` before sampling the used-space statistics, the `live` and `total` storage should also be identical for other measurements.

## Preparing (Use Case) Input Data

In addition to synthetic data, we used data from different datasets, each representing a single use case.

### Used Datasets

* E-Mail: [Enron email dataset](https://www.cs.cmu.edu/~enron/) - Approximately, half a million real emails from 150 users, including mailbox structure.
* Twitter: [twitter7](http://snap.stanford.edu/data/twitter7.html) - unfortunately, the dataset is no longer available.
* IoT Data: Based on [dweet.io](http://dweet.io/) data, containing approximately 1.84 million authentic IoT messages.

### Dataset Entry Layouts

#### Enron Email Dataset

This dataset contains snapshots of users' mailboxes with their folder structure intact, e.g. the first inbox mail is located at `maildir/user-1/inbox/1.` for `user-1`.
Since our provided scripts operate directly on the Enron email dataset, we refer to `precompute-email/precompute_mail.py` for further details.

#### Twitter7 Dataset

The Twitter7 dataset consisted of plain text files containing the individual tweets from 2009 using minimal formatting as follows:

```
total number:3
T	2019-10-30 18:05:23
U	http://twitter.com/user1
W	Tweet content in one line

T	2019-10-30 18:15:00
U	http://twitter.com/user2
W	Tweet content in another line

T	2019-10-30 23:59:59
U	http://twitter.com/user3
W	Tweet content in yet another line
```

#### IoT Data

Our processing pipeline for IoT data relies on information about update frequencies and payload sizes of individual sensor nodes.
To this end, the processing pipeline expects CSV files `*-messagefrequency.csv` and `*-messagesizes.csv` with the following layouts, respectively:

* `*-messagefrequency.csv`: `[node_id];[%Y-%m-%d] [%H:%M:%S.%f]`, where timestamp syntax corresponds to [`strftime` format codes of Python's `datetime`](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)
* `*-messagesizes.csv`: `[node_id];[payload_size_bytes]`

# PRADA: Practical Data Annotations

In past years, cloud storage systems saw an enormous rise in usage.
However, despite their popularity and importance as underlying infrastructure for more complex cloud services, today's cloud storage systems do not account for compliance with regulatory, organizational, or contractual data handling requirements by design.
Since legislation increasingly responds to rising data protection and privacy concerns, complying with data handling requirements becomes a crucial property for cloud storage systems.
We present PRADA, a practical approach to account for compliance with data handling requirements in key-value based cloud storage systems.
To achieve this goal, PRADA introduces a transparent data handling layer, which empowers clients to request specific data handling requirements and enables operators of cloud storage systems to comply with them.
This repository contains our implementation of PRADA on top of the distributed database Cassandra.
Furthermore, we provide information on how to conduct performance evaluations of PRADA-based clusters in [`eval/README.md`](eval/README.md).

## Publications

PRADA was initially presented at IEEE IC2E 2017 in our paper:

* M. Henze, R. Matzutt, J. Hiller, E. Mühmer, J. H. Ziegeldorf, J. van der Giet, K. Wehrle: ["Practical Data Compliance for Cloud Storage"](https://ieeexplore.ieee.org/document/7923809), Proc. IC2E'17, IEEE.

## Installation

We recommend creating a dedicated environment (e.g., [VirtualBox](https://www.virtualbox.org) virtual machines) consisting of Ubuntu 16.04 systems for the setup, because of specific software versions required.
We assume that you performed the basic setup of your cluster nodes or virtual machines and their networked interconnection.

- Clone this repository and `cd` to it
  ```
    mkdir -p ~/prada  # FIXME git clone
    cd ~/prada
  ```
- Install dependencies
  - Optional dependencies
    ```
      sudo apt-get install \
          tcpstat  # For traffic statistics
    ```
- [Download Oracle Java SE Development Kit 7u79](https://www.oracle.com/technetwork/java/javase/downloads/java-archive-downloads-javase7-521261.html), save it to `~/prada/jdk.tar.gz`, then install it
  ```
    mkdir oracle_jdk
    tar -zxf jdk.tar.gz -C ~/prada/oracle_jdk/ --strip 1
    for prg in java javac javaws jar; do sudo update-alternatives --install "/usr/bin/${prg}" "${prg}" "~/prada/oracle_jdk/bin/${prg}" 1; done
    for prg in java javac javaws jar; do sudo update-alternatives --set "${prg}" "~/prada/oracle_jdk/bin/${prg}"; done
  ```
- Install Apache ant 1.9.6
  ```
    wget -O ~/prada/ant.tar.gz https://archive.apache.org/dist/ant/binaries/apache-ant-1.9.6-bin.tar.gz
    mkdir apache_ant
    sudo tar -zxf ant.tar.gz -C ~/prada/apache_ant --strip 1
    update-alternatives --install "/usr/bin/ant" "ant" ~/prada/apache_ant/bin/ant 1
    update-alternatives --set "ant" ~/prada/apache_ant/bin/ant
  ```
- Adapt PRADA's base config file (`examples/prada_config`) and copy it to PRADA's main folder (`~/prada`)
    - `PRADA_SRC_DIR`: Location where you checked out PRADA's repository, in our example `~/prada`
    - `PRADA_OUT_DIR`: Location where you want to have PRADA's data and log files stored, e.g., `~/.prada`
- Adapt (search for `### PRADA Config`), then copy Cassandra's configuration files (`examples/conf/*` to `prada-src/conf/*`)
    - Maintain consistency to `prada_config`
    - `annotations.conf`: PRADA data handling configuration
    - `log4j-tools.properties`, `log4j-server.properties`: Change log location (default `~/.prada/log/system.log`)
    - `cassandra.yaml`: Majority of required adaptions
- Create folders as configured in `~/prada/conf/cassandra.yaml`
  ```
    mkdir -p ~/.prada/log/commitlog
    mkdir -p ~/.prada/saved_caches
  ```
- Compile PRADA
  ```
    cd ~/prada/prada-src
    make
  ```
- Start PRDA
  ```
    cd bin
    prada start
    cd ..
  ```
- Stop PRADA
  ```
    cd bin
    prada stop
    cd ..
  ```

## Acknowledgements

The authors would like to thank Annika Seufert for support with the simulations.
This work has received funding from the European Union's Horizon 2020 research and innovation program 2014–2018 under grant agreement No. 644866 (SSICLOPS) and from the Excellence Initiative of the German federal and state governments.
This article reflects only the authors' views and the funding agencies are not responsible for any use that may be made of the information it contains.

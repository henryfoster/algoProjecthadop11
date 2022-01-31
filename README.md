# M11.1 Programmierkonzepte und Algorithmen WiSe2021/22

# Belegaufgabe 11: Häufigkeit Hadoop

**Prerequironments:**
- Java8
- Maven3
- dataset: https://www.dropbox.com/s/xs404wmj8czg1ow/texts.zip?dl=0 (unzip into texts)
- (Docker)
- (Docker Compose)

Tested on Ubuntu 20.04.

**Install Instructions**

**Hadoop:**
- clone the repo: `git clone https://github.com/big-data-europe/docker-hadoop.git`
- enter the repo: `cd docker-hadoop`
- start the hadoop-cluster:`docker-compose up -d`
- copy dataset files into the namenode: `docker cp texts/ namenode:texts`
- connect to the containers console: `docker exec -it namenode bash`
- create a folder in hdfs: `hadoop fs -mkdir -p input`
- copy the dataset files into hdfs: `hdfs dfs -put texts/ input/`
- verify that the files are in hdfs: `hdfs dfs -ls input/texts/German`

For further instructions' checkout https://github.com/big-data-europe/docker-hadoop


**Hadoop job**
- clone this repo: `git clone https://github.com/henryfoster/algoProjecthadop11.git`
- enter the repo folder: `cd algoProjecthadop11`
- install maven dependencies: `mvn clean install`
- build executable jar: `mvn clean build`

**Run job on Hadoop**
- copy the jar from the target folder into the cluster: `docker cp target/AlgorithmenUndDatenstrukturenAbgabeHadoop-1.0-SNAPSHOT.jar namenode:work.jar`
- connect to the containers console: `docker exec -it namenode bash`
- run the Counter job: `hadoop jar work.jar de.htw.counter.Counter input/texts output`
- verify the output: `hdfs dfs -cat output/part-r-00000`
- run the sorting job: `hadoop jar work.jar de.htw.sorter.Sorter output/part-r-00000 output/output2`
- verify the output
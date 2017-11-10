#CS PDPMR FALL 2017
# SHREYSA SHARMA
# Assignment A8
HADOOP_HOME = $(HOME)/tools/hadoop-2.8.2
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.2.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.2.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.2.jar:out:.

all:	build run

build:	clean compile jar

compile:
	mkdir -p out
	javac -cp $(MY_CLASSPATH) -d out ./src/org/neu/pdpmrA8/*/*.java
	javac -cp $(MY_CLASSPATH) -d out ./src/org/neu/pdpmrA8/*.java

jar:
	jar -cmf src/MANIFEST.MF out/airportAirlineDelay.jar -C out .

run:
	$(HADOOP_HOME)/bin/hdfs dfs -mkdir -p output
	$(HADOOP_HOME)/bin/hadoop jar out/airportAirlineDelay.jar -input=input/airline/*.csv -output=output/a8/
	$(HADOOP_HOME)/bin/hadoop fs -getmerge output/a8/DataCleaning output/results.csv
	Rscript -e 'library(rmarkdown); rmarkdown::render("./report.Rmd", "pdf_document")'

run_small:
	$(HADOOP_HOME)/bin/hdfs dfs -mkdir -p output_small
	$(HADOOP_HOME)/bin/hadoop jar out/airportAirlineDelay.jar -input=input/323.csv -output=output_small/a8/
	$(HADOOP_HOME)/bin/hadoop fs -getmerge output_small/a8/DataCleaning output_small/results.csv

clean:
	rm -rf out
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r -f output/a8
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r -f output_small/a8

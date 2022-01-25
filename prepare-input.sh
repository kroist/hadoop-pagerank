#!/bin/sh

wget http://snap.stanford.edu/data/web-Stanford.txt.gz

gunzip web-Stanford.txt.gz


hadoop fs -mkdir -p input

hadoop fs -put web-Stanford.txt input/

hadoop jar ./pagerank-1.0-SNAPSHOT.jar org.zubec.pagerank.MatrixVector.MatrixVectorIO.PrepareMatrix input/web-Stanford.txt output/parsed_matrix

hadoop fs -rm input/web-Stanford.txt

hadoop fs -get output/parsed_matrix stanford-parsed.txt

split --number=l/4 stanford-parsed.txt
hadoop fs -mkdir input/web-Stanford.txt
hdfs dfs -put xaa input/web-Stanford.txt/
hdfs dfs -put xab input/web-Stanford.txt/
hdfs dfs -put xac input/web-Stanford.txt/
hdfs dfs -put xad input/web-Stanford.txt/

rm xa*


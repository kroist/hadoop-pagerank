#!/bin/bash


# delete old output 
hdfs dfs -rm -r output

# run mapreduce 
hadoop jar ./pagerank-1.0-SNAPSHOT.jar org.zubec.pagerank.PageRank input/web-Stanford.txt output



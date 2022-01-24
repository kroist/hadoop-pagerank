### PageRank implementation in hadoop


Use [kiwenalu/hadoop-cluster-docker](https://github.com/kiwenlau/hadoop-cluster-docker) for running JAR.

Download and unzip SNAP dataset (for example [web-Stanford](http://snap.stanford.edu/data/web-Stanford.txt.gz))

`
    wget http://snap.stanford.edu/data/web-Stanford.txt.gz
    ungzip web-Stanford.txt.gz
`

Put dataset to HDFS
`
    hdfs dfs -mkdir input
    hdfs dfs -put web-Stanford.txt input/
`

Run script
`
    ./run-pagerank.sh
`

Copy output to machine from hdfs:
`
    hdfs dfs -get output/result/part-r-00000 result.txt
`


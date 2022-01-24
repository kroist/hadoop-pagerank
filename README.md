### PageRank implementation in hadoop


Use [kiwenalu/hadoop-cluster-docker](https://github.com/kiwenlau/hadoop-cluster-docker) for running JAR.

Download and unzip SNAP dataset (for example [web-Stanford](http://snap.stanford.edu/data/web-Stanford.txt.gz))

`
    wget http://snap.stanford.edu/data/web-Stanford.txt.gz
    
    gunzip web-Stanford.txt.gz
`

Put dataset to HDFS
`
    hadoop fs -mkdir -p input

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

## Algorithm

We are given with graph transition matrix M.

Our goal is to find vector V, where V(i) is the probability that we reach page i.

The final formula is V = M * V, so we can approximate V starting with V_0, and then

`
    V_{i+1} = normalized (M * V_{i})
`

## Results

On cluster with 5 nodes, total time for 20 iterations is approximately 700 seconds.
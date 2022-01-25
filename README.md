### PageRank implementation in hadoop


Use [kiwenalu/hadoop-cluster-docker](https://github.com/kiwenlau/hadoop-cluster-docker) (set cluster size for 5) for running JAR.

Load dataset to memory using script

```
    ./prepare-input.sh
```


Run mapreduce script
```
    ./run-pagerank.sh
```

Copy output to machine from hdfs:
```
    hdfs dfs -get output/result/part-r-00000 result.txt
```

## Algorithm

We are given with graph transition matrix M.

Our goal is to find vector V, where V(i) is the probability that we reach page i.

The final formula is V = M * V, so we can approximate V starting with V_0, and then

```
   V_{i+1} = normalized (M * V_{i})
```

## Results

On cluster with 5 nodes, total time for 20 iterations is approximately 700 seconds.
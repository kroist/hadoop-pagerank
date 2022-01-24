package org.zubec.pagerank.MatrixVector.MatrixVectorMult;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixVectorMultReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{

    /*
    for aggregated tuple (i, V[j_1], V[j_2], V[j_3], ...) sum up the values to get the V'[i], where V' is new vector.
    */
    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}

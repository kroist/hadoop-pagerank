package org.zubec.pagerank.MatrixVector.MatrixVectorIO;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixInputReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String ans = "";
        for (IntWritable value : values) {
            if (!ans.isEmpty())
                ans += "\t";
            ans += value.toString();
        }
        context.write(key, new Text(ans));
    }
}

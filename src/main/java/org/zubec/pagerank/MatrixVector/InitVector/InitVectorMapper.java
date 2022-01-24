package org.zubec.pagerank.MatrixVector.InitVector;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitVectorMapper extends Mapper<Text, Text, IntWritable, DoubleWritable>{
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Double val = 1.0 / (double)(Integer.parseInt(value.toString()));
        context.write(new IntWritable(Integer.parseInt(key.toString())), new DoubleWritable(val));
    }
}

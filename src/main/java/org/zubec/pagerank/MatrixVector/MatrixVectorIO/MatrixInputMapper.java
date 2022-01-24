package org.zubec.pagerank.MatrixVector.MatrixVectorIO;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixInputMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /* 
            Parse an input adjcacency matrix in SNAP format, which lines format is:

            vertexA<tab>vertexB

        */
        if (value.charAt(0) != '#') {
            int tabIndex = value.find("\t");
            int vertexA = Integer.parseInt(Text.decode(value.getBytes(), 0, tabIndex));
            int vertexB = Integer.parseInt(Text.decode(value.getBytes(), tabIndex+1, value.getLength() - (tabIndex+1)));
            context.write(new IntWritable(vertexA), new IntWritable(vertexB));
        }
    }
}

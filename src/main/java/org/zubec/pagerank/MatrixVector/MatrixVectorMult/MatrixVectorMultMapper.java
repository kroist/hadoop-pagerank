package org.zubec.pagerank.MatrixVector.MatrixVectorMult;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixVectorMultMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

    ArrayList<Double> vec = new ArrayList<>();

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path vectorPath = new Path(conf.get("vectorPath") + "/part-r-00000");
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(vectorPath));
        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        int iter = 0;
        while(reader.next(key, value)) {
            vec.add(value.get());
            ++iter;
            if (iter != key.get()) {
                System.out.println("BAAD");
                System.exit(1);
            }
        }
        reader.close();
    }
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] splt = value.toString().split("\t");
        int i = Integer.parseInt(splt[0]);
        int j = Integer.parseInt(splt[1]);
        if (j == -1)
            context.write(new IntWritable(i), new DoubleWritable(0));
        else
            context.write(new IntWritable(i), new DoubleWritable(vec.get(j-1)));
    }
}

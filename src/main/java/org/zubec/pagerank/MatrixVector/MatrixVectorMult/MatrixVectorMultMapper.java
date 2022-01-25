package org.zubec.pagerank.MatrixVector.MatrixVectorMult;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixVectorMultMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

    HashMap<Integer, Double> vec = new HashMap<>();

    /*
    read current vector into memory
    */
    @Override
    public void setup(Context context) throws IOException {
        
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(conf.get("vectorPath")), true);
        while (fileStatusIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusIterator.next();
            if (fileStatus.getPath().toString() == "" || fileStatus.getPath().toString().endsWith("_SUCCESS"))
                continue;
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fileStatus.getPath()));
            IntWritable key = new IntWritable();
            DoubleWritable value = new DoubleWritable();
            while(reader.next(key, value)) {
                vec.put(key.get(), value.get());
            }
            reader.close();
        }
    }


    /*
    for each tuple (i, j1, j2, j3, ...) map it to (i, V[j1]*1/(J_CNT), ...)
    */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] splt = value.toString().split("\t");
        int i = Integer.parseInt(splt[0]);
        int jCnt = splt.length-2;
        if (jCnt == 0) {
            context.write(new IntWritable(i), new DoubleWritable(0));
        }
        double sum = 0;
        double divisor = 1.0 / (double)jCnt;
        for (int it = 1; it < splt.length; it++) {
            int val = Integer.parseInt(splt[it]);
            if (val != -1)
                sum += vec.get(val) * divisor;
        }
        context.write(new IntWritable(i), new DoubleWritable(sum));
    }
}

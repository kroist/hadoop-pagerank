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
    0 1 0
    1 0 1
    0 1 0

    (i, j, 1/CNT_i)

    for each tuple (i, j) map it to (i, V[j])
    */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] splt = value.toString().split("\t");
        int i = Integer.parseInt(splt[0]);
        int j = Integer.parseInt(splt[1]);
        if (j == -1)
            context.write(new IntWritable(i), new DoubleWritable(-1));
        context.write(new IntWritable(i), new DoubleWritable(vec.get(j)));
    }
}

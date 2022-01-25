package org.zubec.pagerank.MatrixVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class VectorNormalization {
    public static void normalizeVector(String vectorPathStr) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(vectorPathStr), true);
        Double sum = 0.0;
        TreeMap<Integer, Double> mp = new TreeMap<>();
        while (fileStatusIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusIterator.next();
            if (fileStatus.getPath().toString() == "" || fileStatus.getPath().toString().endsWith("_SUCCESS"))
                continue;
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fileStatus.getPath()));
            IntWritable key = new IntWritable();
            DoubleWritable value = new DoubleWritable();
            while(reader.next(key, value)) {
                sum += value.get()*value.get();
                mp.put(key.get(), value.get());
            }
            reader.close();
        }
        Path vectorPath = new Path(vectorPathStr);
        
        Double norm = Math.sqrt(sum);
        fs.delete(vectorPath, true);
        Path path2 = new Path(vectorPathStr + "/part-r-00000");
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path2), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(DoubleWritable.class));
        
        for (Map.Entry<Integer, Double> entry : mp.entrySet()){
            writer.append(new IntWritable(entry.getKey()), new DoubleWritable(entry.getValue()/norm));
        }
        writer.close();
    }
    
}

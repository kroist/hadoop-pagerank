package org.zubec.pagerank.MatrixVector;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class VectorNormalization {
    public static void normalizeVector(String vectorPathStr) throws IOException {
        Configuration conf = new Configuration();
        Path vectorPath = new Path(vectorPathStr + "/part-r-00000");
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(vectorPath));
        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        Double sum = 0.0;
        ArrayList<Double> values = new ArrayList<>();
        while(reader.next(key, value)) {
            sum += value.get()*value.get();
            values.add(value.get());
        }
        reader.close();
        Double norm = Math.sqrt(sum);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(vectorPath, false);
        Path path2 = new Path(vectorPathStr + "/part-r-00000");
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path2), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(DoubleWritable.class));
        int it = 0;
        System.out.println(norm);
        for (Double itValue : values) {
            ++it;
            writer.append(new IntWritable(it), new DoubleWritable(itValue/norm));
        }
        writer.close();
    }
    
}

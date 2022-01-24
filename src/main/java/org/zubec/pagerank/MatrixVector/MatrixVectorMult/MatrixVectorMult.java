package org.zubec.pagerank.MatrixVector.MatrixVectorMult;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.zubec.pagerank.PageRank;

public class MatrixVectorMult {

    /**
     * 
     * @param matrixString - string with path to matrix on hdfs
     * @param vectorInString - string with path to input vector on hdfs
     * @param vectorOutString - string with path to output vector on hdfs
     */
    public static boolean multMatrixVector(String matrixString, String vectorInString, String vectorOutString) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        /*
        set configuration variable with path to input string
        */
        conf.set("vectorPath", vectorInString);

        Job job = Job.getInstance(conf, "Matrix Vector Multiplication");
        job.setJobName("Matrix Vector Multiplication");
        job.setJarByClass(PageRank.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(MatrixVectorMultMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(matrixString));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(MatrixVectorMultReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(vectorOutString));

        return job.waitForCompletion(true);
    }
}

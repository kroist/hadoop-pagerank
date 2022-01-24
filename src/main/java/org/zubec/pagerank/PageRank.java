package org.zubec.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.zubec.pagerank.matrixVector.InitVector.InitVector;
import org.zubec.pagerank.matrixVector.MatrixInput.MatrixInputMapper;

public class PageRank {


    public static Double DUMP_FACTOR = 0.85;
    
    private static int ITER_NUMBER = 2;
    private static String InputPath = "";
    private static String OutputPath = "";

    public static int NodeCount = 0;


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("please specify args[0] - input file, args[1] - output directory");
            System.exit(0);
        }
        InputPath = args[0];
        OutputPath = args[1];

        PageRank pagerank = new PageRank();

        boolean isCompleted;
        isCompleted = pagerank.inputMatrix(InputPath, OutputPath + "/read_matrix");
        if (!isCompleted) {
            System.exit(1);
        }

        InitVector.initVector(OutputPath + "/vector_0", OutputPath + "/read_matrix");

        System.out.println("end.");
        System.exit(1);


    }

    /**
        
    1. Read matrix M[N*N].

    2. Init vector V[N*1]  {1/N, 1/N, ...}

    3. Multiply V by M for K times. 


    */

    public boolean inputMatrix(String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration(), "Input processing");
        job.setJarByClass(PageRank.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MatrixInputMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(in));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        return job.waitForCompletion(true);
    }
}
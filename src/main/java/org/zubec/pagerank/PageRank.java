package org.zubec.pagerank;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.zubec.pagerank.MatrixVector.InitVector.InitVector;
import org.zubec.pagerank.MatrixVector.MatrixVectorIO.MatrixInputMapper;
import org.zubec.pagerank.MatrixVector.MatrixVectorMult.MatrixVectorMult;

public class PageRank {


    public static Double DUMP_FACTOR = 0.85;
    
    private static int ITER_NUMBER = 20;
    private static String InputPath = "";
    private static String OutputPath = "";

    public static int NodeCount = 0;

    public static String vectorPath = "";


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("please specify args[0] - input file, args[1] - output directory");
            System.exit(0);
        }
        InputPath = args[0];
        OutputPath = args[1];


        boolean isCompleted;
        isCompleted = inputMatrix(InputPath, OutputPath + "/read_matrix");
        if (!isCompleted) {
            System.exit(1);
        }

        isCompleted = InitVector.initVector(OutputPath + "/vector_00", OutputPath + "/read_matrix");
        if (!isCompleted) {
            System.exit(1);
        }

        DecimalFormat df = new DecimalFormat("00");

        int cnt_iter = 0;

        for (int it = 1; it <= ITER_NUMBER; it++) {
            String inputVec = OutputPath + "/vector_" + df.format(cnt_iter);
            String outputVec = OutputPath + "/vector_" + df.format(cnt_iter+1);
            ++cnt_iter;
            isCompleted = MatrixVectorMult.multMatrixVector(OutputPath + "/read_matrix", inputVec, outputVec);
            if (!isCompleted) {
                System.exit(1);
            }
        }


        isCompleted = outputVector(OutputPath + "/vector_" + df.format(cnt_iter), OutputPath + "/result");
        if (!isCompleted) {
            System.exit(1);
        }

        System.out.println("end.");
        System.exit(0);


    }

    /**
        
    1. Read matrix M[N*N].

    2. Init vector V[N*1]  {1/N, 1/N, ...}

    3. Multiply V by M for K times. 


    */

    public static boolean inputMatrix(String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration(), "Input processing");
        job.setJobName("Input processing");
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

    public static boolean outputVector(String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration(), "Writing output to file");
        job.setJobName("Writing output to file");
        job.setJarByClass(PageRank.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(in));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));
        return job.waitForCompletion(true);
    }
}
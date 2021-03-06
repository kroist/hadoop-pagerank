package org.zubec.pagerank.MatrixVector.InitVector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.zubec.pagerank.PageRank;

public class InitVector {
    public static boolean initVector(String pathString, String matrixString) throws IOException, InterruptedException, ClassNotFoundException {
        

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path vectorPath = new Path(pathString + "f" + "/part-r-00000");

        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(matrixString), true);
        int N = 0;

        while (fileStatusIterator.hasNext()) {
            FileStatus fileStatus = fileStatusIterator.next();
            if (fileStatus.getPath().toString() == "" || fileStatus.getPath().toString().endsWith("_SUCCESS"))
                continue;
            BufferedReader buf = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
            String line = buf.readLine();
            while(line != null) {
                String[] strSplit = line.split("\t");
                int a = Integer.parseInt(strSplit[0]);
                int b = Integer.parseInt(strSplit[1]);
                if (a > N)
                    N = a;
                if (b > N)
                    N = b;
                line = buf.readLine();
            }
            buf.close();
        }

        
        /*
        find number of nodes iterating over edges
        */
        

        PrintWriter writeVector = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(vectorPath))));

       

        for (int i = 1; i <= N; i++) {
             
            /*
            create pagerank vector
            */
            writeVector.println(i + "\t" + N);
        }
        writeVector.close();

        
        Job job = Job.getInstance(new Configuration(), "Vector Initialization");
        job.setJobName("Vector Initialization");
        job.setJarByClass(PageRank.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(InitVectorMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(pathString + "f"));
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(pathString));

        return job.waitForCompletion(true);

    }
}

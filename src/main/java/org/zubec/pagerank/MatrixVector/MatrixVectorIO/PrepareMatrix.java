package org.zubec.pagerank.MatrixVector.MatrixVectorIO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrepareMatrix {
    public static void prepareMatrix(String in, String out) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path matrixPath = new Path(in);

        int N = 0;
        

        PrintWriter writeMatrix = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(new Path(out)))));

        /*
        find number of nodes iterating over edges
        */
        BufferedReader buf = new BufferedReader(new InputStreamReader(fs.open(matrixPath)));
        String line = buf.readLine();
        while(line != null) {
            if (line.charAt(0) != '#') {
                String[] strSplit = line.split("\t");
                int a = Integer.parseInt(strSplit[0]);
                int b = Integer.parseInt(strSplit[1]);
                if (a > N)
                    N = a;
                if (b > N)
                    N = b;
                line = buf.readLine();
                writeMatrix.println(a + "\t" + b);
            }
        }
        buf.close();
        
        /*
        add fictive edges to assure, that all vertices will be involved in MapReduce
        */
        for (int i = 1; i <= N; i++)
            writeMatrix.println(i + "\t" + "-1");
        writeMatrix.close();
        
    }
}

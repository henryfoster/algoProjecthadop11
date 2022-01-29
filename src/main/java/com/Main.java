package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


// base code from
public class Main {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Counter");

        job.setJarByClass(com.Main.class);
        job.setMapperClass(com.MyMapper.class);
        job.setReducerClass(com.MyReducer.class);
        job.setCombinerClass(com.MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/home/edu/uni/files/input/german"));
        FileInputFormat.addInputPath(job, new Path("/home/edu/uni/files/input/english"));
        // add more path for more folders
        FileSystem fs = FileSystem.get(conf);
        Path outDir = new Path("/home/edu/uni/files/output");


        if(fs.exists(outDir)) {
            System.out.println("Already exists ... overwriting");
            fs.delete(outDir, true);
        }



        FileOutputFormat.setOutputPath(job, new Path("/home/edu/uni/files/output"));

        try {
            job.waitForCompletion(true);
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
        }

        System.out.print("Done!!!");

    }
}

package de;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sorter {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Sorter");

        job.setJarByClass(Sorter.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/home/edu/uni/files/output/part-r-00000"));

        FileSystem fs = FileSystem.get(conf);
        Path outDir = new Path("/home/edu/uni/files/output2");


        if(fs.exists(outDir)) {
            System.out.println("Already exists ... overwriting");
            fs.delete(outDir, true);
        }

        FileOutputFormat.setOutputPath(job, new Path("/home/edu/uni/files/output2"));
        job.setNumReduceTasks(1);
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
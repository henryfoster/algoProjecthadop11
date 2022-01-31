package de.htw.sorter;

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
        // setting up Job
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Sorter");

        job.setJarByClass(Sorter.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // setting up input file
        FileSystem fs = FileSystem.get(conf);

        String inputPath = "/home/edu/uni/files/output/part-r-00000";
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // setting up input path
        String outputPath = "/home/edu/uni/files/output2";
        Path outDir = new Path(outputPath);
        if(fs.exists(outDir)) {
            System.out.println("Already exists ... overwriting");
            fs.delete(outDir, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // starting the job
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException exception) {
            exception.printStackTrace();
        }

        System.out.print("Done!!!");

    }
}
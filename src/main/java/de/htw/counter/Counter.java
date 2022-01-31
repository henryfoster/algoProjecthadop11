package de.htw.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 *  base code from https://moodle.htw-berlin.de/mod/resource/view.php?id=1086055
 *  Counts all word occurrences grouped by subfolder name
 */
public class Counter {

    public static void main(String[] args) throws IOException {
        // setting up Job
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Counter");

        job.setJarByClass(Counter.class);
        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);
        job.setCombinerClass(CounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // add more path for more folders
        FileSystem fs = FileSystem.get(conf);
        String inputPath = args[0]; // "/home/edu/uni/files/texts"

        Path inputDir = new Path(inputPath);
        if(fs.exists(inputDir)) {
            System.out.println("Input dir found!");
            File[] directories = new File(inputPath+"/").listFiles(File::isDirectory);
            if (null == directories) {
                System.out.println("Directories could not be found! " +
                        "(make sure the input directory has following structure dir/dir/file)"
                );
                return;
            }
            // iterating over all subfolders (languages) and adding them to input
            for (File dir: directories) {
                String path = dir.getPath();
                System.out.println(path);
                FileInputFormat.addInputPath(job, new Path(path));
            }
        }

        // setting up output
        String outputPath = args[1]; //  "/home/edu/uni/files/output"
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

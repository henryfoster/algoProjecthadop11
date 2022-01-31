package de.htw.counter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // splits a line of text into an array of words
        String[] tokens = value.toString().split("\\P{L}+");

        // following 3 lines access the folder name where the computed line comes from
        Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        String filename = filePath.getParent().toString();
        String folder = filename.substring(filename.lastIndexOf("/") + 1);
        // iterates over each word and creates a key consisting of the folder name,word
        for(String token: tokens) {
            // excludes empty words
            if (" ".equals(token) || "".equals(token)) {
                continue;
            }
            // adds a key value pair to the context (key: folder,word ; value: 1)
            context.write(new Text(folder + "," +token), new IntWritable(1));
        }
    }
}

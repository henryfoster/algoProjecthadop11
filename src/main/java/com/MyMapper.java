package com;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\P{L}+");

        Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        String filename = filePath.getParent().toString();
        String folder = filename.substring(filename.lastIndexOf("/") + 1);
        for(String token: tokens) {
            context.write(new Text(folder + "," +token), new IntWritable(1));
        }
    }
}

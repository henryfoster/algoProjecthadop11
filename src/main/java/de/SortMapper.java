package de;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

public class SortMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("[\\s,]+");

        context.write(new Text(tokens[0]), new Text(tokens[1] + "," + tokens[2]));
        //context.write(new Text(value), new Text("dawdawd"));
    }
}
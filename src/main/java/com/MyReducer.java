package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {



    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable val: values) {
            count += val.get();
        }
        context.write(new Text(key), new IntWritable(count));

    }
}

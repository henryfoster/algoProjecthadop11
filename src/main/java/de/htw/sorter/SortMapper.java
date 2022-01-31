package de.htw.sorter;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // splitting up folder name (language), word and count into an array of strings [language, word, count]
        String[] tokens = value.toString().split("[\\s,]+");
        // ignoring all words that length are smaller or equals 4
        if (4 >= tokens[1].length()) {
            return;
        }
        // setting the language as key and the word and count as value separated by a comma
        context.write(new Text(tokens[0]), new Text(tokens[1] + "," + tokens[2]));
    }
}
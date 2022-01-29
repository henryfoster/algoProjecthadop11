package de;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SortReducer extends Reducer<Text, Text, Text, Text> {



    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer>input = new HashMap<>();

        for (Text val: values) {
            if (val.toString().contains("{") ) {
                    context.write(key, val);
                    return;
                }
            // seperate values
            String[] word = val.toString().split(",");
            if (input.size() < 10) {
                input.put(word[0], Integer.parseInt(word[1]));
            } else {
                sortByValue(input);
                if ((int) input.values().toArray()[0] < Integer.parseInt(word[1])) {
                    String firstKey = input.keySet().stream().findFirst().get();
                    input.remove(firstKey);
                    input.put(word[0], Integer.parseInt(word[1]));
                }
            }

        }
        context.write(new Text(key), new Text(input.toString()));

    }

    // code from: https://www.geeksforgeeks.org/sorting-a-hashmap-according-to-values/
    public static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

}
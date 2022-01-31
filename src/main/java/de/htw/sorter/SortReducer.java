package de.htw.sorter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class SortReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // map that stores the top ten words for a language
        HashMap<String, Integer>topTen = new HashMap<>();

        for (Text val: values) {
            // separate values into an array [word, count]
            String[] word = val.toString().split(",");
            // add the first 10 words to the map without any condition (
            // if there are <= 10 words then they are topTen by default)
            if (topTen.size() < 10) {
                topTen.put(word[0], Integer.parseInt(word[1]));
            } else {
                // finds the key with the smallest value
                String smallestKey = findSmallestKey(topTen);
                // just to be sure and avoid NullPointerException
                if (null == smallestKey) {
                    continue;
                }
                // if the count of the current word is higher than the smallest entry in the topTen
                // remove the smallest entry and set the new
                if (topTen.get(smallestKey) < Integer.parseInt(word[1])) {
                    topTen.remove(smallestKey);
                    topTen.put(word[0], Integer.parseInt(word[1]));
                }
            }
        }
        // just to sort the top ten map at the end
        topTen = sortByValue(topTen);
        // write back to the context: key = language value = topTen map formatted as string
        context.write(new Text(key), new Text(topTen.toString()));
    }

    // code form: https://stackoverflow.com/a/2776245/9560200
    private static String findSmallestKey(HashMap<String, Integer> map) {
        Map.Entry<String, Integer> min = null;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (min == null || min.getValue() > entry.getValue()) {
                min = entry;
            }
        }
        if (null == min) {
            return null;
        }
        return min.getKey();
    }

    // code from: https://www.geeksforgeeks.org/sorting-a-hashmap-according-to-values/
    private static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm)
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
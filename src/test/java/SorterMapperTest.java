
import de.htw.sorter.SortMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class SorterMapperTest {

    @Test
    public void testSortMapperSuccess() throws IOException {
        new MapDriver<Object, Text, Text, Text>()
                .withMapper(new SortMapper())
                .withInput(new LongWritable(0), new Text("German,testword 50"))
                .withInput(new LongWritable(1), new Text("German,testwordd 20"))
                .withAllOutput(Arrays.asList(
                        new Pair<Text,Text>(new Text("German"),new Text("testword,50")),
                        new Pair<Text,Text>(new Text("German"),new Text("testwordd,20"))
                ))
                .runTest();
    }

    @Test
    public void testSortMapperWordFilter() throws IOException {
        new MapDriver<Object, Text, Text, Text>()
                .withMapper(new SortMapper())
                .withInput(new LongWritable(0), new Text("German,testword 50"))
                .withInput(new LongWritable(1), new Text("German,test 20"))
                .withAllOutput(Arrays.asList(
                        new Pair<Text,Text>(new Text("German"),new Text("testword,50"))
                ))
                .runTest();
    }
}

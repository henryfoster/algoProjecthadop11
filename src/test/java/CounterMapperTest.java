import de.htw.counter.CounterMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class CounterMapperTest {

    @Test
    public void testJUnitSuite()
    {
        assertEquals(1, 1);
    }

    @Test
    public void testCounterMapper() throws IOException {
        new MapDriver<Object, Text, Text, IntWritable>()
                .withMapper(new CounterMapper())
                .withMapInputPath(new Path("texts/German/test3.txt"))
                .withInput(new LongWritable(0), new Text("msg msg msg"))
                .withAllOutput(Arrays.asList(
                        new Pair<Text,IntWritable>(new Text("German,msg"),new IntWritable(1)),
                        new Pair<Text,IntWritable>(new Text("German,msg"),new IntWritable(1)),
                        new Pair<Text,IntWritable>(new Text("German,msg"),new IntWritable(1))
                ))
                .runTest();
    }
}

import de.htw.counter.CounterReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class CounterReducerTest {

    @Test
    public void testSumReducer() throws IOException {
        new ReduceDriver<Text, IntWritable,Text,IntWritable>()
                .withReducer(new CounterReducer())
                .withInput(new Text("msg"), Arrays.asList(new IntWritable(1),new IntWritable(1)))
                .withOutput(new Text("msg"), new IntWritable(2))
                .runTest();
    }
}

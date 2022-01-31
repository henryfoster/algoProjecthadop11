import de.htw.sorter.SortReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SortReducerTest {

    @Test
    public void testSortReducer() throws IOException {
        new ReduceDriver<Text, Text, Text, Text>()
                .withReducer(new SortReducer())
                .withInput(new Text("German"), Arrays.asList(new Text("testwort,50"),new Text("testworthallo,20")))
                .withOutput(new Text("German"), new Text("{testworthallo=20, testwort=50}"))
                .runTest();
    }

    @Test
    public void testSortReducerOrder() throws IOException {
        new ReduceDriver<Text, Text, Text, Text>()
                .withReducer(new SortReducer())
                .withInput(new Text("German"), Arrays.asList(new Text("testworthallo,20"), new Text("testwort,50")))
                .withOutput(new Text("German"), new Text("{testworthallo=20, testwort=50}"))
                .runTest();
    }

    @Test
    public void testSortReducerTopTen() throws IOException {

        new ReduceDriver<Text, Text, Text, Text>()
                .withReducer(new SortReducer())
                .withInput(new Text("German"), Arrays.asList(
                        new Text("testworta,20"),
                        new Text("testwortb,50"),
                        new Text("testwortc,60"),
                        new Text("testwortd,5"),
                        new Text("testworte,20"),
                        new Text("testwortf,50"),
                        new Text("testwortg,1"),
                        new Text("testworth,50"),
                        new Text("testworti,20"),
                        new Text("testwortj,5070"),
                        new Text("testwortk,20"),
                        new Text("testwortl,500"),
                        new Text("testwortm,2"),
                        new Text("testwortn,50")
                ))
                .withOutput(new Text("German"), new Text("{testworti=20, testwortk=20, testworta=20, testwortf=50, testworth=50, testwortn=50, testwortb=50, testwortc=60, testwortl=500, testwortj=5070}"))
                .runTest();
    }
}

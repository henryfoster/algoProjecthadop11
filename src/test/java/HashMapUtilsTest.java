import de.htw.util.HashMapUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HashMapUtilsTest {

    @Test
    public void testFindSmallestKeyFirst() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("Test1", 1);
        map.put("Test2", 2);
        map.put("Test3", 3);
        map.put("Test4", 4);
        assertEquals("Test1", HashMapUtils.findSmallestKey(map));
    }

    @Test
    public void testFindSmallestKeyLast() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("Test1", 1);
        map.put("Test2", 2);
        map.put("Test3", 3);
        map.put("Test0", 0);
        assertEquals("Test0", HashMapUtils.findSmallestKey(map));
    }

    @Test
    public void testFindSmallestKeyMid() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("Test1", 1);
        map.put("Test2", 2);
        map.put("Test3", 0);
        map.put("Test0", 4);
        assertEquals("Test3", HashMapUtils.findSmallestKey(map));
    }

    @Test
    public void testSortByValueSorted() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("Test1", 1);
        map.put("Test2", 2);
        map.put("Test3", 3);
        map.put("Test4", 4);

        Map<String, Integer> sorted = HashMapUtils.sortByValue(map);

        assertEquals("{Test1=1, Test2=2, Test3=3, Test4=4}", sorted.toString());
    }

    @Test
    public void testSortByValueFullyUnsorted() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("Test1", 4);
        map.put("Test2", 3);
        map.put("Test3", 2);
        map.put("Test4", 1);

        Map<String, Integer> sorted = HashMapUtils.sortByValue(map);

        assertEquals("{Test4=1, Test3=2, Test2=3, Test1=4}", sorted.toString());
    }

    @Test
    public void testSortByValuePartiallyUnsorted() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("Test1", 4);
        map.put("Test2", 2);
        map.put("Test3", 3);
        map.put("Test4", 1);

        Map<String, Integer> sorted = HashMapUtils.sortByValue(map);

        assertEquals("{Test4=1, Test2=2, Test3=3, Test1=4}", sorted.toString());
    }
}

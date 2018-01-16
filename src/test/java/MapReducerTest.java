import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MapReducerTest {

public static final Text INPUT_TEXT1 = new Text("ip97 - - [24/Apr/2011:09:33:39 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\"\n" +
        "ip98 - - [24/Apr/2011:09:36:36 -0400] \"GET /robots.txt HTTP/1.1\" 404 286 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"\n" +
        "ip98 - - [24/Apr/2011:09:37:25 -0400] \"GET /docs/rhl-rg-6.1en/s1-modules-cdromparameters.html HTTP/1.1\" 404 325 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"");


    private MapReduceDriver<LongWritable, Text, IntWritable, IPBytesWritable, IntWritable, AverageBytesWritable> mapReduceDriver;

    @Before
    public void setUp() {
        BytesCountMapper mapper = new BytesCountMapper();
        BytesCountReducer reducer = new BytesCountReducer();
        BytesCountCombiner combiner = new BytesCountCombiner();

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setCombiner(combiner);
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(0), INPUT_TEXT1);
        mapReduceDriver.withOutput(new IntWritable(97), new AverageBytesWritable(12550,12550));
        mapReduceDriver.withOutput(new IntWritable(98), new AverageBytesWritable(305.5,611));
        mapReduceDriver.runTest();
    }
}

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MapperTest {

    private MapDriver<LongWritable, Text, IntWritable, IPBytesWritable> mapDriver;

    public static final Text INPUT_TEXT1 = new Text("ip97 - - [24/Apr/2011:09:33:39 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\"\n" +
            "ip98 - - [24/Apr/2011:09:36:36 -0400] \"GET /robots.txt HTTP/1.1\" 404 286 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"\n" +
            "ip98 - - [24/Apr/2011:09:37:25 -0400] \"GET /docs/rhl-rg-6.1en/s1-modules-cdromparameters.html HTTP/1.1\" 404 325 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"");

    public static final Text INPUT_TEXT2 = new Text("ip29 - - [24/Apr/2011:05:41:30 -0400] \"GET /robots.txt HTTP/1.1\" 404 286 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\"\n" +
            "ip28 - - [24/Apr/2011:05:41:56 -0400] \"GET /sun3/ HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"");


    public static final IPBytesWritable OUTPUT_1 = new IPBytesWritable(12550, 1);
    public static final IPBytesWritable OUTPUT_2 = new IPBytesWritable(286, 1);
    public static final IPBytesWritable OUTPUT_3 = new IPBytesWritable(325, 1);
    public static final IPBytesWritable OUTPUT_4 = new IPBytesWritable(0, 1);
    public static final IPBytesWritable OUTPUT_5 = new IPBytesWritable(286, 1);

    @Before
    public void setUp() {
        Mapper mapper = new BytesCountMapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapWithRegularInput() throws IOException {
        mapDriver.withInput(new LongWritable(), INPUT_TEXT1);
        mapDriver.withOutput(new IntWritable(97), OUTPUT_1);
        mapDriver.withOutput(new IntWritable(98), OUTPUT_2);
        mapDriver.withOutput(new IntWritable(98), OUTPUT_3);
        mapDriver.runTest();
    }

    @Test
    public void TestMapWithStringContainsEmptyBytesValue() throws IOException{
        mapDriver.withInput(new LongWritable(), INPUT_TEXT2);
        mapDriver.withOutput(new IntWritable(29), OUTPUT_5);
        mapDriver.withOutput(new IntWritable(28), OUTPUT_4);
        mapDriver.runTest();
    }
}

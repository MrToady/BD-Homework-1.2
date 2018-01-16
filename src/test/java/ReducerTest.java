import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerTest {
    public static final IntWritable IP = new IntWritable(3);
    public static final IPBytesWritable INPUT_1 = new IPBytesWritable(12550, 1);
    public static final IPBytesWritable INPUT_2 = new IPBytesWritable(286, 1);
    public static final IPBytesWritable INPUT_3 = new IPBytesWritable(325, 1);
    public static final IPBytesWritable INPUT_4 = new IPBytesWritable(5523, 17);
    public static final IPBytesWritable INPUT_5 = new IPBytesWritable(362525, 13);
    public static final AverageBytesWritable OUTPUT_1 = new AverageBytesWritable(4387, 13161);
    public static final AverageBytesWritable OUTPUT_2 = new AverageBytesWritable(12268.266666666666, 368048);

    private ReduceDriver<IntWritable, IPBytesWritable, IntWritable, AverageBytesWritable> reduceDriver;
    private List<IPBytesWritable> valuesList;

    @Before
    public void setUp() {
        Reducer reducer = new BytesCountReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {

        valuesList = new ArrayList<>();
        valuesList.add(INPUT_1);
        valuesList.add(INPUT_2);
        valuesList.add(INPUT_3);

        reduceDriver.setInput(IP, valuesList);
        reduceDriver.withOutput(IP, OUTPUT_1);
        reduceDriver.runTest();
    }

    @Test
    public void testReducerWithPluralRequests() throws IOException {

        valuesList = new ArrayList<>();
        valuesList.add(INPUT_4);
        valuesList.add(INPUT_5);

        reduceDriver.setInput(IP, valuesList);
        reduceDriver.withOutput(IP, OUTPUT_2);
        reduceDriver.runTest();
    }
}

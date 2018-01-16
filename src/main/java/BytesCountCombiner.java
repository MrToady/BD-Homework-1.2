import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesCountCombiner extends Reducer<IntWritable, IPBytesWritable, IntWritable, IPBytesWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IPBytesWritable> values, Context context) throws IOException, InterruptedException {
        int requestSum = 0;
        long bytesSum = 0;

        for (IPBytesWritable value : values) {
            requestSum += value.getRequests();
            bytesSum += value.getBytes();
        }
        context.write(key, new IPBytesWritable(bytesSum, requestSum));
    }
}

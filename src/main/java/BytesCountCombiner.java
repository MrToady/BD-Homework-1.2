import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Implementation of combiner class
 * Summarizes bytes and request amount and writes it in IPBytesWritable
 * Result is written in context
 *
 * @see {@link IPBytesWritable}
 */
public class BytesCountCombiner extends Reducer<IntWritable, IPBytesWritable, IntWritable, IPBytesWritable> {
    private IPBytesWritable writable = new IPBytesWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IPBytesWritable> values, Context context) throws IOException, InterruptedException {
        int requestSum = 0;
        long bytesSum = 0;

        for (IPBytesWritable value : values) {
            requestSum += value.getRequests();
            bytesSum += value.getBytes();
        }
        writable.setBytes(bytesSum);
        writable.setRequests(requestSum);
        context.write(key, writable);
    }
}

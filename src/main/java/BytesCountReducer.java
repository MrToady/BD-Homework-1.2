import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Summarizes bytes got from all values
 * Gets amount of requests
 * Divides total bytes sum on total request sum and writes AverageBytesWritable in context
 *
 * @see {@link AverageBytesWritable}
 */
public class BytesCountReducer extends Reducer<IntWritable, IPBytesWritable, IntWritable, AverageBytesWritable> {
    private AverageBytesWritable averageBytesWritable = new AverageBytesWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IPBytesWritable> values, Context context) throws IOException, InterruptedException {

        int requestSum = 0;
        long totalBytesSum = 0;

        for (IPBytesWritable value : values) {
            requestSum += value.getRequests();
            totalBytesSum += value.getBytes();
        }

        double averageBytes = (double) totalBytesSum / requestSum;
        averageBytesWritable.setAverageBytes(averageBytes);
        averageBytesWritable.setTotalBytes(totalBytesSum);
        context.write(key, averageBytesWritable);
    }
}

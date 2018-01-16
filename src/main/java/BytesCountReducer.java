import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesCountReducer extends Reducer<IntWritable, IPBytesWritable, IntWritable, AverageBytesWritable>{
    @Override
    protected void reduce(IntWritable key, Iterable<IPBytesWritable> values, Context context) throws IOException, InterruptedException {

        int requestSum = 0;
        long totalBytesSum = 0;

        for (IPBytesWritable value: values) {
            requestSum += value.getRequests();
            totalBytesSum += value.getBytes();
        }

        double averageBytes = (double) totalBytesSum/requestSum;
        context.write(key, new AverageBytesWritable(averageBytes ,totalBytesSum));
    }
}

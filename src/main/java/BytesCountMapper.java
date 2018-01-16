import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class BytesCountMapper extends Mapper<LongWritable, Text, IntWritable, IPBytesWritable> {
    public static final String delimiter = "\n";
    public static final String PROTOCOL_VERSION = "HTTP/1.";
    public static final int OFFSET = 14;
    public static final int IP_OFFSET = 2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), delimiter);
        while (tokenizer.hasMoreTokens()) {
            String log = tokenizer.nextToken();
            int ip = Integer.valueOf(log.substring(IP_OFFSET, log.indexOf(' ')));
            int byteStart = log.indexOf(PROTOCOL_VERSION) + OFFSET;
            String bytesString = log.substring(byteStart, log.indexOf(' ', byteStart));

            long bytes;
            try {
                bytes = Long.valueOf(bytesString);
            } catch (NumberFormatException e) {
                bytes = 0;
            }

            context.write(new IntWritable(ip), new IPBytesWritable(bytes, 1));
        }
    }
}

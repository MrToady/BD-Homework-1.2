import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class BytesCountMapper extends Mapper<LongWritable, Text, IntWritable, IPBytesWritable> {
    private final UserAgentStringParser userAgentStringParser = UADetectorServiceFactory.getResourceModuleParser();
    private static final String DELIMITER = "\n";
    private static final String PROTOCOL_VERSION = "HTTP/1.";
    private static final int OFFSET = 14;
    private static final int IP_OFFSET = 2;
    private static final String USER_AGENT_GROUP_NAME = "UserAgent";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), DELIMITER);
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

            context.getCounter(USER_AGENT_GROUP_NAME, getUserAgent(log))
                    .increment(1);

            context.write(new IntWritable(ip), new IPBytesWritable(bytes, 1));
        }
    }

    private String getUserAgent(String log) {
        int length = log.length();
        int startOfUAInfo = log.lastIndexOf('"', length - 2) + 1;
        String userAgentInfo = log.substring(startOfUAInfo, length - 1);

        return userAgentStringParser.parse(userAgentInfo).getName();
    }
}

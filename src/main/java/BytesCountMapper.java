import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BytesCountMapper extends Mapper<LongWritable, Text, IntWritable, IPBytesWritable> {
    private final UserAgentStringParser userAgentStringParser = UADetectorServiceFactory.getResourceModuleParser();
    private IPBytesWritable ipBytesWritable = new IPBytesWritable();
    private IntWritable ipNumber = new IntWritable();
    private static final String PROTOCOL_VERSION = "HTTP/1.";
    private static final int OFFSET = 14;
    private static final int IP_OFFSET = 2;
    private static final String USER_AGENT_GROUP_NAME = "UserAgent";

    /**
     * Parses input string to separate words according to delimiter
     * Gets integer value of IP number as key
     * Gets long value of bytes and writes it in custom ipBytesWritable
     *
     * @see {@link IPBytesWritable}
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String log = value.toString();
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

        ipNumber.set(ip);
        ipBytesWritable.setBytes(bytes);
        ipBytesWritable.setRequests(1);
        context.write(ipNumber, ipBytesWritable);
    }

    /**
     * Parses log string and find User Agent part
     *
     * @return user agent name
     */
    private String getUserAgent(String log) {
        int length = log.length();
        int startOfUAInfo = log.lastIndexOf('"', length - 2) + 1;
        String userAgentInfo = log.substring(startOfUAInfo, length - 1);

        return userAgentStringParser.parse(userAgentInfo).getName();
    }
}

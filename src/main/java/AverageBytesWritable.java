import lombok.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements custom writable for final output
 */
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Getter
@Setter
public class AverageBytesWritable implements Writable {
    private double averageBytes;
    private long totalBytes;

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeDouble(averageBytes);
        output.writeLong(totalBytes);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        averageBytes = input.readDouble();
        totalBytes = input.readLong();
    }

    @Override
    public String toString() {
        return averageBytes + "," + totalBytes;
    }
}
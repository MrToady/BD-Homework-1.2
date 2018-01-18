import lombok.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements custom writable for intermediate processing
 */
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Getter
@Setter

public class IPBytesWritable implements Writable {
    private long bytes;
    private int requests;

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(bytes);
        output.writeInt(requests);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        bytes = input.readLong();
        requests = input.readInt();
    }
}

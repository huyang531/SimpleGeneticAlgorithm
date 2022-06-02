import javax.annotation.Nonnull;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

/**
 * Since this class is used as key for Mappers, it must implement WritableComparable<> but not simply
 * ArrayWritable (which only extends Writable but not Comparable).
 */
public class LongArrayWritable implements WritableComparable<LongArrayWritable> {
    private LongWritable[] values;
    private static final Random r = new Random(System.nanoTime());


    public LongArrayWritable(LongWritable[] values) {
        this.values = values.clone();
    }

    public LongArrayWritable() {
    }

    public LongWritable[] get() {
        return values;
    }

    public void set(LongWritable[] values) {
        this.values = values;
    }

    @Override
    public String toString() {
       StringBuilder str = new StringBuilder();
       for (int i = 0; i < values.length; i++) {
           int limit = (i == values.length - 1 ? MyDriver.GENE_LEN_REMAINDER : MyDriver.LONG_BITS);
           long mask = 1L;
           for (int j = 0; j < limit; j++, mask <<= 1) {
               str.append((values[i].get() & mask) == 0 ? '0' : '1');
           }
           str.append(" | ");
       }
       return str.toString();
    }

    /**
     * Return random results so that individuals are randomly distributed. Even the identical ones.
     * @param o not important anymore
     * @return a random result
     */
    public int compareTo(@Nonnull LongArrayWritable o) {
        return r.nextBoolean() ? 1 : -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(values.length);
        for (LongWritable lw : values) {
            lw.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        values = new LongWritable[dataInput.readInt()];
        for (int i = 0; i < values.length; i++) {
            LongWritable value = new LongWritable();
            value.readFields(dataInput);
            values[i] = value;
        }
    }
}

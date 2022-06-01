import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {
    public LongArrayWritable(LongWritable[] values) {
        super(LongWritable.class, values);
    }

    public LongArrayWritable() {
        super(LongWritable.class);
    }

    @Override
    public LongWritable[] get() {
        return (LongWritable[]) super.get();
    }

    @Override
    public String toString() {
       StringBuilder str = new StringBuilder();
       for (LongWritable value : this.get()) {
           str.append(value.get()).append("|");
       }
       return str.toString();
    }
}

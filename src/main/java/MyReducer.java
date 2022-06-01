import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {
}

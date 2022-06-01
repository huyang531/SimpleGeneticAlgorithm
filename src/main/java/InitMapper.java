import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Random;

public class InitMapper extends Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {

    Random rng = new Random(System.nanoTime());
    LongWritable[] individual = new LongWritable[MyDriver.LONGS_PER_ARRAY];
}

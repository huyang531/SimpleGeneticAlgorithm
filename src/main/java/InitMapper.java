import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class InitMapper extends Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {

    public Random rng = new Random(System.nanoTime());
    public LongWritable[] individual = new LongWritable[MyDriver.LONGS_PER_ARRAY];

    @Override
    public void map(LongArrayWritable key, LongWritable value, Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        int num = (int) Math.ceil((double) MyDriver.BITS_PER_MAPPER / MyDriver.weights.size());
        // generate initial individuals
        for (int i = 0; i < num; i++) {
            for (int j = 0; j < MyDriver.LONGS_PER_ARRAY; j++) {
                long g = 0;
                for (int k = 0; k < MyDriver.LONG_BITS; k++) {
                    g = g | (rng.nextBoolean() ? 0 : 1);
                    if (k != MyDriver.LONG_BITS - 1) g <<= 1; // don't shift the last bit
                }
                individual[j] = new LongWritable(g);
            }
            context.write(new LongArrayWritable(individual), new LongWritable(0));
        }
    }
}

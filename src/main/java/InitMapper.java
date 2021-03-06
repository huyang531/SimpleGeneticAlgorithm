import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * The Mapper used to generate the initial population.
 */
public class InitMapper extends Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {

    public Random rng = new Random(System.nanoTime());
    public LongWritable[] individual = new LongWritable[MyDriver.LONGS_PER_ARRAY];
    public static double pOnes = 0.5;

    /**
     * This method will generate num individuals randomly using bit-wise operation (which is more efficient). It will
     * take in some dummy input which is not important.
     *
     * @param key not important
     * @param value not important
     * @param context context of current Map task
     * @throws IOException Hadoop operations may throw this exception
     * @throws InterruptedException Hadoop operations may throw this exception
     */
    @Override
    public void map(LongArrayWritable key, LongWritable value, Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        int num = (int) Math.ceil((double) MyDriver.BITS_PER_MAPPER / MyDriver.weights.size());
        // generate initial individuals
        for (int i = 0; i < num; i++) {
            generateOneIndividual(individual, rng);
            context.write(new LongArrayWritable(individual), new LongWritable(0));
        }
    }

    public static void generateOneIndividual(LongWritable[] individual, Random rng) {
        for (int j = 0; j < MyDriver.LONGS_PER_ARRAY; j++) {
            long g = 0;
            for (int k = 0; k < MyDriver.LONG_BITS; k++) {
                g = g | (rng.nextDouble() > pOnes ? 0 : 1);
                if (k != MyDriver.LONG_BITS - 1) g <<= 1; // don't shift the last bit
            }
            individual[j] = new LongWritable(g);
        }
    }
}

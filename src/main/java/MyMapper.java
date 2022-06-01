import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {
    long max = -1;
    long fitness = 0;

    public long getFitness(LongWritable[] individual) {
        long f = 0;
        for (int i = 0; i < individual.length; i++) {
            for (int j = 0; j < MyDriver.LONG_BITS; j++) {
                f += MyDriver.weights.get(i * MyDriver.LONG_BITS + j);
            }
        }
        return f;
    }

    @Override
    public void map(LongArrayWritable key, LongWritable value, Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        // calculate fitness
        LongWritable[] individual = key.get();
        fitness = getFitness(individual);
    }
}

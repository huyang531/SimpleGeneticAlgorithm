import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

/**
 * This is the Partitioner for the Genetic Algorithm. It will distribute the intermediate keypair totally randomly among
 * the Reducers.
 */
public class MyPartitioner extends Partitioner<LongArrayWritable, LongWritable> {
    Random rng = new Random(System.nanoTime());

    @Override
    public int getPartition(LongArrayWritable longArrayWritable, LongWritable longWritable, int i) {
        return (Math.abs(rng.nextInt()) % i);
    }
}

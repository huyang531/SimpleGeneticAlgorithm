import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;

/**
 * The Genetic Algorithm's Mapper.
 * Input keypair: <Individual's gene, dummy data>
 * Output keypair: <Individual's gene, fitness>
 */
public class MyMapper extends Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {
    public Random rng = new Random(System.nanoTime());
    public long maxFitness = -1;
    public LongArrayWritable maxIndividual;
    public long fitness = 0;

    /**
     * Get the fitness of an individual, aka the total weight of the current selection. During trial run, if the total
     * weight exceeded the knapsack's capacity, randomly remove items from the selection until the weight is below the
     * capacity. This is to get an estimated proportion of 1s' in the gene.
     * @param individual the individual to evaluate (and possibly mutate?)
     * @param trial whether this is called from the trail run at the beginning to determine the proportion of 1s'
     * @param rng this needs to be explicitly passed here to bypass the nature of static methods. (This method has to be
     *            static because it needs to be called from other classes)
     * @return the fitness of the individual
     */
    public static long getFitness(LongWritable[] individual, boolean trial, Random rng) {
        Vector<Integer> ones = new Vector<>();

        // get current knapsack value
        long weight = 0;
        long fitness = 0;
        for (int i = 0; i < individual.length; i++) {
            long mask = 1L;
            int limit = (i == individual.length - 1 ? MyDriver.GENE_LEN_REMAINDER : MyDriver.LONG_BITS);
            for (int j = 0; j < limit; j++) {
                int index = i * MyDriver.LONG_BITS + j;
                if ((individual[i].get() & mask) != 0) {
                    ones.add(index);
                    weight += MyDriver.weights.get(index);
                    fitness += MyDriver.values.get(index);
                }
                mask <<= 1;
            }
        }

        if (!trial && weight > MyDriver.capacity) return 0;

        // if knapsack overflow (sneaky evolve)
        while (weight > MyDriver.capacity) {
            // randomly take out one item
            int randomIndex = rng.nextInt(ones.size());
            int takeOutIndex = ones.get(randomIndex);
            ones.remove(randomIndex);

            long minuend = 1L << (takeOutIndex % MyDriver.LONG_BITS);
            individual[takeOutIndex / MyDriver.LONG_BITS].set(individual[takeOutIndex / MyDriver.LONG_BITS].get() - minuend);
            weight -= MyDriver.weights.get(takeOutIndex);
            fitness -= MyDriver.values.get(takeOutIndex);
        }

        return fitness;
    }

    /**
     * In the map() method, calculate the fitness of each input individual and keep track of the best one (which will be
     * later written to a file).
     * @param key the individual
     * @param value dummy fitness (doesn't matter)
     * @param context the context of this Map task
     * @throws IOException context.write() might throw this exception
     * @throws InterruptedException context.write() might throw this exception
     */
    @Override
    public void map(LongArrayWritable key, LongWritable value, Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        // calculate fitness
        LongWritable[] individual = key.get();
        fitness = getFitness(individual, false, rng);

        // keep track of the best individual to file
        if (fitness > maxFitness) {
            maxFitness = fitness;
            maxIndividual = new LongArrayWritable(individual);
        }

        context.write(key, new LongWritable(fitness));
    }

    /**
     * In the cleanup() method, write the best individual alongside its fitness to a file. The controller (MyDriver)
     * will read the files from all Mappers at the end of each iteration to determine if convergence is reached and
     * whether to start the next iteration.
     * @param context the context of this Map task
     * @throws IOException file operations and Hadoop tasks may throw this exception
     * @throws InterruptedException Hadoop tasks may throw this exception
     */
    @Override
    public void cleanup(Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        // write the best individual to file
        Path globalResultDir = new Path(MyDriver.tmpDir, MyDriver.GLOBAL_MAP_RESULT_DIR);
        Path outFile = new Path(globalResultDir, "res-" + context.getTaskAttemptID().getTaskID().toString());

        SequenceFile.Writer.Option optionFile = SequenceFile.Writer.file(outFile);
        SequenceFile.Writer.Option optionKey = SequenceFile.Writer.keyClass(LongArrayWritable.class);
        SequenceFile.Writer.Option optionValue = SequenceFile.Writer.valueClass(LongWritable.class);
        SequenceFile.Writer writer = SequenceFile.createWriter(context.getConfiguration(), optionFile, optionKey, optionValue);

        writer.append(maxIndividual, new LongWritable(maxFitness));

        writer.close();
        super.cleanup(context);
    }
}

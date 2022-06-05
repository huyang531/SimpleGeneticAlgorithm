import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

public class MyDriver {
    public static int GENE_LEN_REMAINDER; // equals geneLen % LONG_BITS
    public static int LONGS_PER_ARRAY; // number of longs in an array needed to represent a gene
    public static final int LONG_BITS = 64; // number of bits in a Long
    public static final String ROOT_DIR = "/Users/huyang/Hadoop_runtime"; // root directory for temp files
    public static final String GLOBAL_MAP_RESULT_DIR = "map-results"; // directory to store results
    public static long BITS_PER_MAPPER = 9999L; // DUMMY number of bits an initial mapper can handle
    public static long pop_;

    public static final Vector<Long> weights = new Vector<>();
    public static final Vector<Long> values = new Vector<>();
    public static long capacity;

    public static Path tmpDir;
    public static Path inputDir;
    public static Path outputDir;

    public static long lastMax;
    public static int converged = 0;
    public static int convergenceThreshold;

    public static final long programStartTime = System.currentTimeMillis();

    public static void getProportionOfOnes(int geneLen) {
        Random rng = new Random(System.nanoTime());
        long numOnes = 0, mask = 1;

        LongWritable[][] individuals = new LongWritable[geneLen][LONGS_PER_ARRAY];
        for (int i = 0; i < geneLen; i++) {
            // generate geneLen individuals and get fitness
            InitMapper.generateOneIndividual(individuals[i], rng);
            MyMapper.getFitness(individuals[i], true, rng);

            // get number of ones
            for (int j = 0; j < LONGS_PER_ARRAY; j++) {
                for (int k = 0; k < LONG_BITS; k++, mask <<= 1) {
                    if ((individuals[i][j].get() & mask) != 0) {
                        numOnes++;
                    }
                }
            }
        }

        // get proportion of ones and set MyReducer.pMutationPerBit and InitMapper.pOnes
        double proportionOfOnes = (double) numOnes / (geneLen * geneLen);
        if (proportionOfOnes < MyReducer.pMutationPerBit) {
            MyReducer.pMutationPerBit =  proportionOfOnes;
        }
        InitMapper.pOnes = proportionOfOnes;
    }

    /**
     * Launch and control the tasks. This method controls the overall flow. It will start multiple MapReduce tasks. The
     * first task is to generate the initial population using InitialMapper and an Identity Reducer. Then, it will start
     * no more than <maxIterations> iterations to evolve and mutate the population. After each iteration, the program
     * will go through all the best individuals in the current population by reading files in the GLOBAL_MAP_RESULT_DIR.
     * The program will exit either when convergence is reached or <maxIterations> is reached.
     *
     * @param nReducers     number of Reducers
     * @param geneLen       length of an individual's gene
     * @param maxIterations max number of iterations
     * @param pop           number of initial population
     * @return status code
     * @throws IOException many Hadoop methods throw this exception
     */
    public static int launch(int nReducers, int geneLen, int maxIterations, int pop) throws IOException, InterruptedException, ClassNotFoundException {
        LONGS_PER_ARRAY = (int) Math.ceil((double) geneLen / LONG_BITS);
        GENE_LEN_REMAINDER = geneLen % LONG_BITS;
        int it = 0;
        pop_ = pop;
        int nMappers = (int) Math.ceil((double) pop * geneLen / BITS_PER_MAPPER); // number of initial mappers needed
        long startTime = 0;

        getProportionOfOnes(geneLen);

        while (true) {
            startTime = System.currentTimeMillis();
            // create job and config
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "0-1 Knapsack Problem Iter " + it);

            // set config
            job.setSpeculativeExecution(true);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputKeyClass(LongArrayWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

//            conf.set("ga.longsPerArray", String.valueOf(LONGS_PER_ARRAY));

            job.setPartitionerClass(MyPartitioner.class);

            // set input and output dir
            tmpDir = new Path(ROOT_DIR + "/GeneticAlgoRuntimeTmp");
            inputDir = new Path(tmpDir, "iter_" + it);
            outputDir = new Path(tmpDir, "iter_" + (it + 1));
            FileInputFormat.setInputPaths(job, inputDir);
            FileOutputFormat.setOutputPath(job, outputDir);

            FileSystem fs = FileSystem.get(conf);
            if (it == 0) { // initialization
                System.out.println("[INFO] Program started.");
                System.out.println("[INFO] Number of Mappers: " + nMappers);
                System.out.println("[INFO] Number of Reducers: " + nReducers);
                System.out.println("[INFO] Convergence Threshold: " + convergenceThreshold);
                System.out.println("[WARNING] Existing files from temporary directory will be deleted: " + tmpDir);

                // write nMappers files to the fs in order to control the number of mappers
                fs.delete(tmpDir, true);
                for (int i = 0; i < nMappers; i++) {
                    Path file = new Path(inputDir, "dummy-file-" + String.format("%05d", i));
                    SequenceFile.Writer.Option optionFile = SequenceFile.Writer.file(file);
                    SequenceFile.Writer.Option optionKey = SequenceFile.Writer.keyClass(LongArrayWritable.class);
                    SequenceFile.Writer.Option optionValue = SequenceFile.Writer.valueClass(LongWritable.class);
                    SequenceFile.Writer writer = SequenceFile.createWriter(conf, optionFile, optionKey, optionValue);

                    LongWritable[] individual = new LongWritable[1];
                    individual[0] = new LongWritable(0);
                    writer.append(new LongArrayWritable(individual), new LongWritable(0));
                    writer.close();
                }

                // set job
                job.setMapperClass(InitMapper.class);
                job.setReducerClass(Reducer.class);
                job.setNumReduceTasks(0);
                System.out.println("[INFO] Generating initial population...");
            } else { // real GA tasks
                // set jobs
                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);
                job.setNumReduceTasks(nReducers);

                // delete deprecated files and temp files from last run
                fs.delete(outputDir, true);
                fs.delete(new Path(tmpDir, GLOBAL_MAP_RESULT_DIR), true);
                System.out.println("[INFO] Running iteration " + it + "...");
            }

            // run job
            if (!job.waitForCompletion(true)) return -1;

            if (it == 0) System.out.println("[INFO] Finished generating initial population!");

            // At the end of each job, find global best individual
            LongWritable currFitness = new LongWritable();
            LongArrayWritable currIndividual = new LongArrayWritable();
            LongWritable maxFitness = new LongWritable(-1);
            LongArrayWritable maxIndividual = new LongArrayWritable();

            if (it > 0) {
                // delete deprecated files
                fs.delete(new Path(tmpDir, "iter_" + (it - 1)), true);
                Path global = new Path(tmpDir, GLOBAL_MAP_RESULT_DIR);
                FileStatus[] fileStatuses = fs.listStatus(global);

                for (FileStatus fileStatus : fileStatuses) {
                    Path inFile = fileStatus.getPath();
                    SequenceFile.Reader.Option optionFile = SequenceFile.Reader.file(inFile);
                    SequenceFile.Reader reader = new SequenceFile.Reader(conf, optionFile);

                    // find best individual
                    while (reader.next(currIndividual, currFitness)) {
                        if (currFitness.get() > maxFitness.get()) {
                            maxFitness.set(currFitness.get());
                            maxIndividual.set(currIndividual.get());

                            // track convergence
                            if (maxFitness.get() == lastMax) {
                                converged++;
                            } else {
                                converged = 0;
                            }
                            lastMax = maxFitness.get();
                        }
                    }

                    reader.close();
                }

                // get the best individual's weight
                long maxWeight= 0;
                for (int i = 0; i < maxIndividual.get().length; i++) {
                    long mask = 1L;
                    int limit = (i == maxIndividual.get().length - 1 ? GENE_LEN_REMAINDER : LONG_BITS);
                    for (int j = 0; j < limit; j++) {
                        int index = i * LONG_BITS + j;
                        if ((maxIndividual.get()[i].get() & mask) != 0) {
                            maxWeight += weights.get(index);
                        }
                        mask <<= 1;
                    }
                }

                // pretty print result
                System.out.println("-------------------------");
                System.out.println("Iteration " + it);
                System.out.println("-------------------------");
                System.out.println("Population: " + pop);
                System.out.println("Gene Length: " + geneLen);
                System.out.println("Best Individual: " + maxIndividual);
                System.out.println("Best Fitness: " + maxFitness);
                System.out.println("Best Weight: " + maxWeight);
                System.out.println("Time Taken: " + (System.currentTimeMillis() - startTime) + "ms\n");

                // check if convergence or maxIterations is reached
                if (it > maxIterations) {
                    System.out.println("[INFO] Maximum number of iteration is reached. Job terminated.");
                    System.out.println("[INFO] Program run time: " + (System.currentTimeMillis() - programStartTime) / 1000 + "s");
                    break;
                } else if (converged >= convergenceThreshold) {
                    System.out.println("[INFO] Convergence is reached. Job terminated.");
                    System.out.println("[INFO] Program run time: " + (System.currentTimeMillis() - programStartTime) / 1000 + "s");
                    break;
                }
            }

            // delete deprecated files
            if (it > 0) {

            }

            it++;
        }

        return 0;
    }

    /**
     * Execute the program.
     * @param args four arguments are expected:
     *             nReducers: number of Reducers; this determines how "parallel" the program is — it should neither be
     *                  too high (bad crossover) nor too low (bad performance)
     *             inputFile: the 0-1 knapsack problem; the first Long is the capacity of the knapsack, followed by a
     *                  list of items' weights
     *             maxIterations: the maximum number of iteration of the Genetic Algorithm; this is to prevent the
     *                  program running endlessly. It should neither be too small nor too large.
     *             popTimesNlogN: this is a coefficient that determines how large the initial population will be. The
     *                  formula is: initial population = popTimesNlogN * geneLen * log(2, geneLen)
     * @throws IOException file operations and Hadoop operations may throw this exception
     * @throws InterruptedException Hadoop operations may throw this exception
     * @throws ClassNotFoundException Hadoop operations may throw this exception
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 4) {
            System.err.println("Usage: GeneticAlgorithm <nReducers> <inputFile> <maxIterations> <popTimesNlogN>");
            System.exit(-1);
        }

        // read args
        int nReducers = Integer.parseInt(args[0]);
        Path inputPath = new Path(args[1]);
        int maxIterations = Integer.parseInt(args[2]);

        // read file and determine geneLen
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = fs.open(inputPath);
        Scanner scanner = new Scanner(in);
        capacity = scanner.nextLong();
        while (scanner.hasNextLong()) {
            weights.add(scanner.nextLong());
            values.add(scanner.nextLong());
        }
        int geneLen = weights.size();


        // determine population and convergence
        int pop = (int) Math.ceil(Integer.parseInt(args[3]) * geneLen * Math.log(geneLen) / Math.log(2));
        convergenceThreshold = (int) Math.ceil(geneLen * 0.8);

        // adjust BITS_PER_MAPPER
        if (geneLen > BITS_PER_MAPPER) BITS_PER_MAPPER = (int) Math.ceil(geneLen * pop * 0.17);

        // clear runtime
        fs.delete(new Path(ROOT_DIR), true);

        System.exit(launch(nReducers, geneLen, maxIterations, pop));
    }


}

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
import java.util.Scanner;
import java.util.Vector;

public class MyDriver {
    public static int GENE_LEN_REMAINDER; // equals geneLen % LONG_BITS
    public static int LONGS_PER_ARRAY; // number of longs in an array needed to represent a gene
    public static final int LONG_BITS = 64; // number of bits in a Long
    public static final String ROOT_DIR = "/Users/huyang/Desktop/Courses/大数据原理与技术/final_project/SimpleGeneticAlgorithm/runtime"; // root directory for temp files
    public static final String GLOBAL_MAP_RESULT_DIR = "map-results"; // directory to store results
    public static final long BITS_PER_MAPPER = 200L; // TODO find number of bits an initial mapper can handle

    public static final Vector<Integer> weights = new Vector<>();
    public static final Vector<Integer> values = new Vector<>();
    public static long capacity;

    public static Path tmpDir;
    public static Path inputDir;
    public static Path outputDir;

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
        int nMappers = (int) Math.ceil((double) pop * geneLen / BITS_PER_MAPPER); // number of initial mappers needed

        while (true) {
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
                // write nMappers files to the fs in order to control the number of mappers
                fs.delete(tmpDir, true);
                System.out.println("[INFO] Initialized GeneticAlgoRuntimeTmp");
                for (int i = 0; i < nMappers; i++) {
                    Path file = new Path(inputDir, "dummy-file-" + String.format("%05d", i));
                    // TODO: simplify dummy file input
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
//                job.setReducerClass(Reducer.class);
                job.setNumReduceTasks(0);
            } else { // real GA tasks
                // set job
                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);
                job.setNumReduceTasks(nReducers);

                // delete deprecated files and temp files from last run
                fs.delete(outputDir, true);
                fs.delete(new Path(tmpDir, GLOBAL_MAP_RESULT_DIR), true);
            }

            // run job
            if (!job.waitForCompletion(true)) return -1;

            // At the end of each job, find global best individual
            LongWritable currFitness = new LongWritable();
            LongArrayWritable currIndividual = new LongArrayWritable();
            LongWritable maxFitness = new LongWritable(-1);
            LongArrayWritable maxIndividual = new LongArrayWritable();

            if (it > 0) {
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
                        }
                    }

                    reader.close();
                }

                // pretty print result
                System.out.println("-------------------------");
                System.out.println("Iteration " + it);
                System.out.println("-------------------------");
                System.out.println("Population: " + pop);
                System.out.println("Best Individual: " + maxIndividual);
                System.out.println("Best Fitness: " + maxFitness);
                System.out.println("Time Taken: " + (System.currentTimeMillis() - job.getStartTime()) + "ms");
                System.out.println("");

                if (it > maxIterations) break; // TODO check if convergence is reached
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
        while (scanner.hasNextLine()) {
            if (scanner.hasNextInt()) { // prevent reading empty line
                weights.add(scanner.nextInt());
                values.add(scanner.nextInt());
            }
        }
        int geneLen = weights.size();

        int pop = (int) Math.ceil(Integer.parseInt(args[3]) * geneLen * Math.log(geneLen) / Math.log(2));

        // clear runtime
        fs.delete(new Path(ROOT_DIR), true);

        System.exit(launch(nReducers, geneLen, maxIterations, pop));
    }


}

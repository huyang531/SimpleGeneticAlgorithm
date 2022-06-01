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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.Vector;

public class MyDriver {
    public static int LONGS_PER_ARRAY; // number of longs in an array needed to represent a gene
    public static final int LONG_BITS = 64; // number of bits in a Long
    public static final String ROOT_DIR = "/home/huyang"; // root directory for temp files
    public static final String GLOBAL_MAP_RESULT_DIR = "/map-results"; // directory to store results
    public static final long BITS_PER_MAPPER = 999999L; // TODO find number of bits an initial mapper can handle

    public static final Vector<Integer> weights = new Vector<>();

    /**
     * Launch and control the task.
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
        int it = 0;
        int nMappers = (int) Math.ceil((double) pop * geneLen / BITS_PER_MAPPER); // number of initial mappers needed

        while (true) {
            // create job and config
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "0-1 Knapsack Problem");

            // set config
            job.setSpeculativeExecution(true);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputKeyClass(LongArrayWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

//            conf.set("ga.longsPerArray", String.valueOf(LONGS_PER_ARRAY));

            job.setPartitionerClass(MyPartitioner.class);

            // set input and output dir
            Path tmpDir = new Path(ROOT_DIR + "GeneticAlgoRuntimeTmp");
            Path inputDir = new Path(tmpDir, "iter_" + it);
            Path outputDir = new Path(tmpDir, "iter_" + (it + 1));
            FileInputFormat.setInputPaths(job, inputDir);
            FileOutputFormat.setOutputPath(job, outputDir);

            FileSystem fs = FileSystem.get(conf);
            if (it == 0) { // initialization
                // write nMappers files to the fs in order to control the number of mappers
                fs.delete(tmpDir, true);
                System.out.println("[INFO] Initialized GeneticAlgoRuntimeTmp");
                for (int i = 0; i < nMappers; i++) {
                    Path file = new Path(inputDir, "part-" + String.format("%05d", i));
                    // TODO: simplify dummy file input
                    SequenceFile.Writer.Option optionFile = SequenceFile.Writer.file(file);
                    SequenceFile.Writer.Option optionKey = SequenceFile.Writer.keyClass(LongArrayWritable.class);
                    SequenceFile.Writer.Option optionValue = SequenceFile.Writer.valueClass(LongWritable.class);
                    SequenceFile.Writer writer = SequenceFile.createWriter(conf, optionFile, optionKey, optionValue);

                    LongWritable[] individual = new LongWritable[1];
                    writer.append(new LongArrayWritable(individual), new LongWritable(0));
                    writer.close();
                }

                // set job
                job.setMapperClass(InitMapper.class);
                job.setReducerClass(Reducer.class);
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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 5) {
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
        while (scanner.hasNextInt()) weights.add(scanner.nextInt());
        int geneLen = weights.size();

        // initial population = popTimesNlogN * geneLen * log(2, geneLen)
        int pop = (int) Math.ceil(Integer.parseInt(args[3]) * geneLen * Math.log(geneLen) / Math.log(2));

        System.exit(launch(nReducers, geneLen, maxIterations, pop));
    }


}

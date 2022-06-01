import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class MyReducer extends Reducer<LongArrayWritable, LongWritable,
        LongArrayWritable, LongWritable> {
    public static final int LONG_BITS = 64; // 单个Long中基因长度
    int tournamentSize = 5; // 巡回赛窗口大小
    int LONGS_PER_ARRAY; // 基因长度包含多少个Long
    LongWritable[][] tournamentInd; // 巡回赛的基因窗口列表
    long[] tournamentFitness = new long[2*tournamentSize]; // 巡回赛的适应度窗口列表
    int processedIndividuals=0; // 已处理的个体数量
    LongArrayWritable[] ind = new LongArrayWritable[2]; // 执行crossover的个体暂存列表

    double pCrossover = 0.2;
    double pMutation = 0.1;
    Random rng;
    int pop = 1;
    MyReducer() {
        rng = new Random(System.nanoTime());
    }

    void crossover() {
        //Perform uniform crossover
        LongWritable[] ind1 = ind[0].get();
        LongWritable[] ind2 = ind[1].get();
        LongWritable[] newInd1 = new LongWritable[LONGS_PER_ARRAY];
        LongWritable[] newInd2 = new LongWritable[LONGS_PER_ARRAY];
        // 不发生交换
        if (rng.nextDouble() > pCrossover) return;
        // 发生交换
        for (int i = 0; i < LONGS_PER_ARRAY; i++) {
            long i1 = 0, i2 = 0, mask = 1;
            for (int j = 0; j < LONG_BITS; j++) {
                if (rng.nextDouble() > 0.5) {
                    i2 |= ind2[i].get() & mask;
                    i1 |= ind1[i].get() & mask;
                } else {
                    i1 |= ind2[i].get() & mask;
                    i2 |= ind1[i].get() & mask;
                }
                mask = mask << 1;
            }
            newInd1[i] = new LongWritable(i1);
            newInd2[i] = new LongWritable(i2);
        }
        ind[0] = new LongArrayWritable(newInd1);
        ind[1] = new LongArrayWritable(newInd2);
    }

    LongWritable[] tournament(int startIndex) {
        // Tournament selection without replacement
        LongWritable[] tournamentWinner = null;
        long tournamentMaxFitness = -1;
        for(int j=startIndex; j<tournamentSize + startIndex; j++) {
            if(tournamentFitness[j] > tournamentMaxFitness) {
                tournamentMaxFitness = tournamentFitness[j];
                tournamentWinner = tournamentInd[j];
            }
        }
        return tournamentWinner;
    }

//    Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context _output;

    @Override
    public void reduce(LongArrayWritable key,
                       Iterator<LongWritable> values,
                       Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context output)
            throws IOException, InterruptedException {
//        _output = output;
        LONGS_PER_ARRAY = Integer.parseInt(output.getConfiguration().get("ga.longsPerArray"));
        tournamentInd = new LongWritable[2*tournamentSize][LONGS_PER_ARRAY];
        pop = Integer.parseInt(output.getConfiguration().get("ga.populationPerMapper"));

        while(values.hasNext()) {
            long fitness = values.next().get();
            tournamentInd[processedIndividuals%tournamentSize] = key.get();
            tournamentFitness[processedIndividuals%tournamentSize] = fitness;

            if ( processedIndividuals < tournamentSize ) {
                // Wait for individuals to join in the tournament and put them for the last round
                tournamentInd[processedIndividuals%tournamentSize + tournamentSize] = key.get();
                tournamentFitness[processedIndividuals%tournamentSize + tournamentSize] = fitness;
            } else {
                // Conduct a tournament over the past window
                ind[processedIndividuals%2] = new LongArrayWritable(tournament(processedIndividuals));

                if ((processedIndividuals - tournamentSize) %2 == 1) {
                    // Do crossover every odd iteration between successive individuals
                    crossover();
                    output.write(ind[0], new LongWritable(0));
                    output.write(ind[1], new LongWritable(0));
                }
            }
            processedIndividuals++;
        }
        if(processedIndividuals == pop - 1) {
//            closeAndWrite();
        }
    }

//    public void closeAndWrite() throws IOException, InterruptedException {
//        System.out.println("Closing reducer");
//        // Cleanup for the last window of tournament
//        for(int k=0; k<tournamentSize; k++) {
//            // Conduct a tournament over the past window
//            ind[processedIndividuals%2] = new LongArrayWritable(tournament(processedIndividuals));
//
//            if ((processedIndividuals - tournamentSize) %2 == 1) {
//                crossover();
//                try {
//                    _output.write(ind[0], new LongWritable(0));
//                    _output.write(ind[1], new LongWritable(0));
//                } catch (InterruptedException e) {
//                    System.err.println("Exception in collector of reducer");
//                    e.printStackTrace();
//                }
//            }
//            processedIndividuals++;
//        }
//    }

}

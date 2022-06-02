import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

/**
 * 处理所有发送到本节点的个体，使用tournament进行选择，并进行杂交和突变。
 *
 */
public class MyReducer extends Reducer<LongArrayWritable, LongWritable,
        LongArrayWritable, LongWritable> {
    int tournamentSize = 5; // 巡回赛窗口大小
    LongWritable[][] tournamentInd; // 巡回赛的基因窗口列表
    long[] tournamentFitness = new long[2*tournamentSize]; // 巡回赛的适应度窗口列表
    int processedIndividuals=0; // 已处理的个体数量
    LongArrayWritable[] ind = new LongArrayWritable[2]; // 执行crossover的个体暂存列表

    double pCrossover = 0.3; // 交换发生的概率
    double pCrossoverPerBit = 0.5; // 交换发生时，每一位发生交换的概率
    public static double pMutation = 0.2; // 突变发生的概率 (0.2)
    public final static double originalP = 0.2;
    double pMutationPerBit = 0.00005; // 突变发生时，每一位发生突变的概率 (0.1)
//    double pMutationPerBit = 0.05; // 突变发生时，每一位发生突变的概率 (0.1)
    Random rng;

    /**
     * 获取参数和初始化部分内容
     */
    @Override
    public void setup(Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context) {
        rng = new Random(System.nanoTime());
        tournamentInd = new LongWritable[2*tournamentSize][MyDriver.LONGS_PER_ARRAY];
        if (MyDriver.converged % 5 == 0 && MyDriver.converged != 0) {
            pMutation = 0.79;
            System.out.println("[WARNING] pMutation REST!!! pMutation: " + pMutation);
        }

        if (pMutation != originalP && MyDriver.converged == 0) {
            pMutation = originalP;
            System.out.println("[WARNING] pMutation REST!!! pMutation: " + pMutation);
        }
    }

    /**
     * 将ind数组中的两个基因进行交换操作
     */
    void crossover() {
        //Perform uniform crossover
        LongWritable[] ind1 = ind[0].get();
        LongWritable[] ind2 = ind[1].get();
        LongWritable[] newInd1 = new LongWritable[MyDriver.LONGS_PER_ARRAY];
        LongWritable[] newInd2 = new LongWritable[MyDriver.LONGS_PER_ARRAY];
        // 不发生交换
        if (rng.nextDouble() > pCrossover) return;
        // 发生交换
        for (int i = 0; i < MyDriver.LONGS_PER_ARRAY; i++) {
            long i1 = 0, i2 = 0, mask = 1;
            for (int j = 0; j < MyDriver.LONG_BITS; j++) {
                if (rng.nextDouble() < pCrossoverPerBit) {
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

    /**
     * 将ind数组中的两个基因进行突变操作
     */
    void mutation() {
        for (int in = 0; in < ind.length; in++) {
            if (rng.nextDouble() < pMutation) { // 进行突变
                LongWritable[] newInd1 = new LongWritable[MyDriver.LONGS_PER_ARRAY];
                for (int i = 0; i < MyDriver.LONGS_PER_ARRAY; i++) { // 每个long
                    long i1 = 0, mask = 1;
                    for (int j = 0; j < MyDriver.LONG_BITS; j++) { // long中每一位
                        if (rng.nextDouble() < pMutationPerBit) { // 进行突变
                            i1 |= ~ind[in].get()[i].get() & mask;
                        } else {
                            i1 |= ind[in].get()[i].get() & mask;
                        }
                        mask = mask << 1;
                    }
                    newInd1[i] = new LongWritable(i1);
                }
                ind[in] = new LongArrayWritable(newInd1);
            }
        }
    }

    /**
     *
     */
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

    @Override
    public void reduce(LongArrayWritable key,
                       Iterable<LongWritable> values,
                       Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context)
            throws IOException, InterruptedException {

        for (LongWritable value : values) {
            if (processedIndividuals < tournamentSize) {
                tournamentInd[processedIndividuals] = key.get();
                tournamentFitness[processedIndividuals] = value.get();
                tournamentInd[processedIndividuals + tournamentSize] = key.get();
                tournamentFitness[processedIndividuals + tournamentSize] = value.get();
            } else {
                System.arraycopy(tournamentInd, 1, tournamentInd, 0, tournamentSize -1);
                System.arraycopy(tournamentFitness, 1, tournamentFitness, 0, tournamentSize -1);
                tournamentInd[tournamentSize - 1] = key.get();
                tournamentFitness[tournamentSize - 1] = value.get();
                int index = processedIndividuals % 2;
                ind[index] = new LongArrayWritable(tournament(0));
                if (index != tournamentSize % 2) {
                    crossover();
                    mutation();
                    context.write(ind[0], new LongWritable(0));
                    context.write(ind[1], new LongWritable(0));
                }
            }
            processedIndividuals++;
        }

    }

    @Override
    public void cleanup(Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable>.Context context)
            throws IOException, InterruptedException {
        for (int i = 1; i <= tournamentSize; i++) {
            int index = (processedIndividuals + i) % 2;
            ind[index] = new LongArrayWritable(tournament(i));
            if (index == tournamentSize % 2) {
                crossover();
                mutation();
                context.write(ind[0], new LongWritable(0));
                context.write(ind[1], new LongWritable(0));
            }
        }
        if (processedIndividuals % 2 == 1) {
            context.write(ind[(processedIndividuals + tournamentSize) % 2], new LongWritable(0));
        }

    }


}

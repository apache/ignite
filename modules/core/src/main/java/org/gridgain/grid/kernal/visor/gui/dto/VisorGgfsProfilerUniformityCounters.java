/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.gui.dto.VisorGgfsProfiler.*;

/**
 * Class to support uniformity calculation.
 *
 * <a href="http://en.wikipedia.org/wiki/Coefficient_of_variation">
 * Uniformity calculated as coefficient of variation.
 * </a>
 *
 * Count read frequency for each file and compare with ideal uniform distribution.
 */
public class VisorGgfsProfilerUniformityCounters implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Analyzed file size in bytes. */
    private long fileSize = 0;

    /** Current block size to calculate uniformity. */
    private long blockSize = UNIFORMITY_DFLT_BLOCK_SIZE;

    private ArrayList<Integer> counters = new ArrayList<>();

    private long calcBlockSize(long fileSize) {
        return Math.max(UNIFORMITY_DFLT_BLOCK_SIZE, fileSize / UNIFORMITY_BLOCKS);
    }

    /**
     * Check if counters and blockSize should be adjusted according to file size.
     *
     * @param newFileSize New size of file.
     */
    public void invalidate(long newFileSize) {
        if (newFileSize < fileSize) { // If newFileSize is less than current fileSize then clear counters.
            fileSize = newFileSize;

            counters.clear();

            blockSize = calcBlockSize(fileSize);
        }
        else if (newFileSize > fileSize) // If newFileSize is bigger then current fileSize then adjust counters and blockSize.
            compact(newFileSize);
    }

    /**
     * Perform compacting counters if {@code newBlockSize} is great more than twice then compact previous counters.
     *
     * @param newFileSize New file size to check.
     */
    private void compact(long newFileSize) {
        long newBlockSize = calcBlockSize(newFileSize);

        if (counters.isEmpty())
            blockSize = newBlockSize;
        else {
            if (newBlockSize >= 2 * blockSize) {
                int ratio = (int)(newBlockSize / blockSize);

                ArrayList<Integer> compacted = new ArrayList<>();

                int sum = 0;
                int cnt = 0;

                for (Integer counter : counters) {
                    sum += counter;
                    cnt++;

                    if (cnt >= ratio) {
                        compacted.add(sum);

                        sum = 0;
                        cnt = 0;
                    }
                }

                if (sum > 0)
                    compacted.add(sum);

                counters.clear();
                counters.addAll(compacted);

                blockSize = newBlockSize;
            }
        }

        fileSize = newFileSize;
    }

    /**
     * Ensure counters capacity and initial state.
     *
     * @param minCapacity Desired minimum capacity.
     */
    private void capacity(int minCapacity) {
        counters.ensureCapacity(minCapacity);

        while(counters.size() < minCapacity)
            counters.add(0);
    }

    /**
     * Increment counters by given pos and length.
     *
     * @param pos Position in file.
     * @param len Read data length.
     */
    public void increment(long pos, long len) {
        int blockFrom = (int)(pos / blockSize);
        int blockTo = (int)((pos + len) / blockSize) + 1;

        capacity(blockTo);

        for (int i = blockFrom; i < blockTo; i++)
            counters.set(i, counters.get(i) + 1);
    }

    /**
     * Add given counters.
     *
     * @param other Counters to add.
     */
    public void aggregate(VisorGgfsProfilerUniformityCounters other) {
        if (fileSize < other.fileSize)
            compact(other.fileSize);
        else if (fileSize > other.fileSize)
            other.compact(fileSize);

        int cnt = other.counters.size();

        if (counters.size() < cnt)
            capacity(cnt);

        for (int i = 0; i < cnt; i++)
            counters.set(i, counters.get(i) + other.counters.get(i));
    }

    /**
     * Calculate uniformity as standard deviation.
     * See: http://en.wikipedia.org/wiki/Standard_deviation.
     *
     * @return Uniformity value as number in {@code 0..1} range.
     */
    public double calc() {
        if (counters.isEmpty())
            return -1;
        else {
            int cap = (int) (fileSize / blockSize + (fileSize % blockSize > 0 ? 1 : 0));

            capacity(cap);

            int sz = counters.size();

            int n = F.sumInt(counters);

            double mean = 1.0 / sz;

            // Calc standard deviation for block read frequency: SQRT(SUM(Freq_i - Mean)^2 / K)
            // where Mean = 1 / K
            //       K - Number of blocks
            //       Freq_i = Counter_i / N
            //       N - Sum of all counters
            double sigma = 0;

            for (Integer counter : counters)
                sigma += Math.pow(counter.doubleValue() / n - mean, 2);

            sigma = Math.sqrt(sigma / sz);

            // Calc uniformity coefficient.
            return 1.0 - sigma;
        }
    }
}

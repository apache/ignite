/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import java.io.*;
import java.util.ArrayList;

/**
 * Class to support uniformity calculation.
 *
 * <a href="http://en.wikipedia.org/wiki/Coefficient_of_variation">
 * Uniformity calculated as coefficient of variation.
 * </a>
 */
public class VisorGgfsProfilerUniformityCounters implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private long fileSize = 0;
    private long blockSize = 0; // TODO UNIFORMITY_DFLT_BLOCK_SIZE
    private ArrayList<Integer> counters = new ArrayList<>();

    private long calcBlockSize(long fileSize) {
        return 0; // TODO  math.max(UNIFORMITY_DFLT_BLOCK_SIZE, fileSize / UNIFORMITY_BLOCKS)
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
        else if (newFileSize > fileSize) { // If newFileSize is bigger then current fileSize then adjust counters and blockSize.
            compact(newFileSize);
        }
    }

    /**
     * Perform compacting counters if `newBlockSize` is great more than twice then compact previous counters.
     *
     * @param newFileSize New file size to check.
     */
    private void compact(long newFileSize) {
        long newBlockSize = calcBlockSize(newFileSize);

        if (!counters.isEmpty()) {
            if (newBlockSize >= 2 * blockSize) {
                int ratio = (int)(newBlockSize / blockSize);

                ArrayList<Integer> compacted = new ArrayList<>(); // TODO counters.grouped(ratio).map(_.sum) // Perform compact.
                        //TODO .toArray // We need to convert to array, because .grouped() is a lazy wrapper on counters.

                counters.clear();
                counters.addAll(compacted);

                blockSize = newBlockSize;
            }
        }
        else
            blockSize = newBlockSize;

        fileSize = newFileSize;
    }

    private void capacity(int c) {
        counters.ensureCapacity(c);
    }

    public void increment(long pos, long len) {
        int blockFrom = (int)(pos / blockSize);
        int blockTo = (int)((pos + len) / blockSize) + 1;

        capacity(blockTo);

        for (int i = blockFrom; i < blockTo; i++)
            counters.set(i, counters.get(i));
    }

    public void aggregate(VisorGgfsProfilerUniformityCounters other) {
        if (fileSize < other.fileSize)
            compact(other.fileSize);
        else if (fileSize > other.fileSize)
            other.compact(fileSize);

        int cnt = other.counters.size();

        if (counters.size() < cnt)
            capacity(cnt);

// TODO        for (i <- 0 until cnt)
//            counters(i) += other.counters(i);
    }

    /**
     * Calculate uniformity as standard deviation.
     * See: http://en.wikipedia.org/wiki/Standard_deviation.
     *
     * @return Uniformity value as number in `0..1` range.
     */
    public double calc() {
        if (!counters.isEmpty()) {
            int cap = (int) (fileSize / blockSize + (fileSize % blockSize > 0 ? 1 : 0));

            capacity(cap);

            int sz = counters.size();

            int n = 0; // TODO counters.sum

            double mean = 1.0 / sz;

            // Calc standard deviation for block read frequency: SQRT(SUM(Freq_i - Mean)^2 / K)
            // where Mean = 1 / K
            //       K - Number of blocks
            //       Freq_i = Counter_i / N
            //       N - Sum of all counters
            double sigma = 0; // TODO Math.sqrt(counters.map(x => math.pow(x.toDouble / n - mean, 2)).sum / sz);

            // Calc uniformity coefficient.
            return 1.0 - sigma;
        }
        else
            return -1;
    }
}

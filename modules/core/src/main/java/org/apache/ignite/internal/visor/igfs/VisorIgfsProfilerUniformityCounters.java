/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.igfs;

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.visor.igfs.VisorIgfsProfiler.UNIFORMITY_BLOCKS;
import static org.apache.ignite.internal.visor.igfs.VisorIgfsProfiler.UNIFORMITY_DFLT_BLOCK_SIZE;

/**
 * Class to support uniformity calculation.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Coefficient_of_variation">Uniformity calculated as coefficient of variation.</a>
 * </p>
 * Count read frequency for each file and compare with ideal uniform distribution.
 */
public class VisorIgfsProfilerUniformityCounters implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Analyzed file size in bytes. */
    private long fileSize;

    /** Current block size to calculate uniformity. */
    private long blockSize = UNIFORMITY_DFLT_BLOCK_SIZE;

    /** Collection of calculated counters. */
    private final ArrayList<Integer> counters = new ArrayList<>();

    /**
     * Calculate block size.
     *
     * @param fileSize File size in bytes.
     * @return Block size.
     */
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
     * @param minCap Desired minimum capacity.
     */
    private void capacity(int minCap) {
        counters.ensureCapacity(minCap);

        while (counters.size() < minCap)
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
    public void aggregate(VisorIgfsProfilerUniformityCounters other) {
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
     * Calculate uniformity as standard deviation. See: http://en.wikipedia.org/wiki/Standard_deviation.
     *
     * @return Uniformity value as number in {@code 0..1} range.
     */
    public double calc() {
        if (counters.isEmpty())
            return -1;
        else {
            int cap = (int)(fileSize / blockSize + (fileSize % blockSize > 0 ? 1 : 0));

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
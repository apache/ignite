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

package org.apache.ignite.internal.processors.hadoop.impl.shuffle.collections;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopSkipList;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.io.GridDataInput;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Skip list tests.
 */
public class HadoopSkipListSelfTest extends HadoopAbstractMapTest {
    /**
     * Smallest positive double, used as an epsilon-threshold.
     */
    static final double epsilon = Double.MIN_NORMAL;

    /**
     * Total number of levels to consider.
     * The level may be from 0 to 32 inclusive, that is, 33 levels in total.
     */
    private static final int LEVELS = 33;

    /**
     * Total number of random values taken per one experimental distribution.
     */
    private static final int VALUES_PER_DISTRIBUTION = 10_000;

    /**
     * Total number of experimental distributions built.
     */
    private static final int SAMPLE_SIZE = 700; // The size of one sample.

    /**
     * Number of samples.
     */
    private static final int SAMPLES = 1000;

    /**
     * Z for failure probability 1E-2.
     */
    @SuppressWarnings("UnusedDeclaration")
    static final double Z_0_99 = 2.326348;

    /**
     * Z for failure probability 1E-3.
     */
    @SuppressWarnings("UnusedDeclaration")
    static final double Z_0_999 = 3.090232;

    /**
     * Z for failure probability 1E-4.
     */
    @SuppressWarnings("UnusedDeclaration")
    static final double Z_0_9999 = 3.719015;

    /**
     * Z for failure probability 1E-5.
     */
    @SuppressWarnings("UnusedDeclaration")
    static final double Z_0_99999 = 4.264897;

    /**
     * Z for failure probability 1E-6.
     */
    static final double Z_0_999999 = 4.753408;

    /**
     * Creates data structure for the level.
     *
     * @param valuesPerDistribution How many values used.
     * @param level The level index.
     * @return The level data.
     */
    static LevelData createLevelData(long valuesPerDistribution, int level) {
        // Expected average distribution count for the given level:
        double mean = mu(valuesPerDistribution, level);

        // Expected sigma (variance) of the k-th level distribution:
        double sigma = sigma(valuesPerDistribution, level);

        return new LevelData(level, mean, sigma);
    }

    /**
     * Makes one geometric distribution.
     *
     * @param levelsCnts Number of levels.
     * @param rnd The random.
     * @param valuesPerDistribution Number of values used to build the distribution. Each value adds one count
     * to some level.
     */
    static void makeDistribution(LevelData[] levelsCnts, Random rnd, long valuesPerDistribution) {
        final int[] counts = new int[levelsCnts.length];

        for (long i = 0; i < valuesPerDistribution; i++) {
            int level = HadoopSkipList.randomLevel(rnd);

            assert level >= 0;

            // NB: In theory, any value is possible but
            // we ignore levels higher than the number of processed levels:
            if (level < levelsCnts.length)
                counts[level]++;
        }

        // Store distribution data:
        for (int level = 0; level < levelsCnts.length; level++)
            levelsCnts[level].singleSampleStatistics.addValue(counts[level]);
    }

    /**
     * Checks correctness of the geometric distribution calculation based on
     * pseudo-random long value.
     *
     * Note that Z-score Z_0_999999 is chosen to provide test failure probability
     * of not more than 5% for 1000 consequent runs.
     */
    public void testLevel() {
        doTestLevelStat(LEVELS, VALUES_PER_DISTRIBUTION, SAMPLE_SIZE, SAMPLES);
    }

    /**
     * Geometric distribution test implementation.
     *
     * @param levels Number of levels to assert.
     * @param valuesPerDistribution How many counts used to build one geometric distribution.
     * @param sampleSize How many distributions used to build one sample.
     * @param samples How many samples used for summary statistics.
     */
    private void doTestLevelStat(final int levels, final int valuesPerDistribution, final int sampleSize,
            final int samples) {
        final Random rnd = new GridRandom();

        // Data array shared between all samples:
        final LevelData[] levelsCnts = new LevelData[levels];

        // Initialize data:
        for (int level = 0; level < levelsCnts.length; level++)
            levelsCnts[level] = createLevelData(valuesPerDistribution, level);

        for (int sampleIdx=0; sampleIdx < samples; sampleIdx++) {
            for (LevelData data: levelsCnts)
                data.resetSampleStatistics();

            // Build one sample ('sampleSize' geometric distributions):
            for (int r = 0; r < sampleSize; r++)
                makeDistribution(levelsCnts, rnd, valuesPerDistribution);

            // Check level:
            for (LevelData data: levelsCnts) {
                final ValueStatistics oneSampleStat = data.singleSampleStatistics;

                oneSampleStat.calculate();

                assert oneSampleStat.getCount() == sampleSize;

                double av = oneSampleStat.getAverage();

                data.getSampleAverageStatistics().addValue(av); // Add this stat to the cumulative statistics.
            }
        }

        System.out.println(" ============================== Statistics over " + samples + " samples:");

        for (int level = 0; level < levelsCnts.length; level++) {
            final LevelData data = levelsCnts[level];

            final ValueStatistics<Double> sampleAverageStat = data.getSampleAverageStatistics();

            sampleAverageStat.calculate();

            assert sampleAverageStat.getCount() == samples;

            double averageDelta = data.mu - sampleAverageStat.getAverage();

            double actualSigma = sampleAverageStat.getStandardDeviation();

            // As CLT (Central Limit Theorem) states:
            final double predictedSigma = data.sigma / Math.sqrt(sampleSize);

            double sigmaRelativeDiffPercent = (actualSigma > predictedSigma)
                ? (100d * (actualSigma - predictedSigma)) / predictedSigma : 0;

            // As the "Z-interval for a mean" states:
            double averageConfidenceErr999999 = Z_0_999999 * predictedSigma / Math.sqrt(samples);

            String msg = "Level = " + level
                + ", mu (ideal average) = " + data.mu
                + ", actual average = " + sampleAverageStat.getAverage()
                + ", actual sigma = " + actualSigma
                + ", predicted sigma (sigma0 / sqrt(n))= " + predictedSigma
                + ", sigma relative diff = " + sigmaRelativeDiffPercent + "%"
                + ", average abs diff = " + averageDelta
                + ", averageConfidenceErr = " + averageConfidenceErr999999
                + ", sigmaCorridor = " + 100d * Math.abs(averageDelta) / averageConfidenceErr999999 + "%";

            System.out.println(msg);

            assertTrue(msg, Math.abs(averageDelta) < averageConfidenceErr999999);
        }
    }

    /**
     * Utility structure to hold the level data.
     */
    private static class LevelData {
        /** The level index. */
        final int level;

        /** The predicted average. */
        final double mu;

        /** The predicted sigma. */
        final double sigma;

        /** */
        private ValueStatistics<Integer> singleSampleStatistics;

        /** */
        private final ValueStatistics<Double> manySamplesAverageStatistics = new ValueStatistics<>();

        /**
         * Constructor.
         *
         * @param level The level.
         * @param mu The average.
         * @param sigma The ideal deviation.
         */
        LevelData(int level, double mu, double sigma) {
            this.level = level;
            this.mu = mu;
            this.sigma = sigma;

            resetSampleStatistics();
        }

        /**
         * Resets the statistics.
         */
        public void resetSampleStatistics() {
            singleSampleStatistics = new ValueStatistics<>();
        }

        /**
         * @return The statistics over all the samples previously added.
         */
        public ValueStatistics<Double> getSampleAverageStatistics() {
            return manySamplesAverageStatistics;
        }
    }

    /**
     * Utility function giving probability
     * to have exactly 'v' counts in level 'k'
     * with total counts being 'n'.
     *
     * @param n Total counts used in distribution.
     * @param k The level index.
     * @param v The value we calculate probability of.
     * @return The probability.
     */
    static double ln_pv(long n, long k, long v) {
        final double p = 1d / pow2(k + 1);

        double res = ln_binomial(n, v);

        final double ln_1_p;

        if (k > 24)
            ln_1_p = -p; // reasonable approximation for very small 'p'.
        else
            ln_1_p = Math.log(1d - p);

        res += (n - v) * ln_1_p;

        res += - v * (k + 1) * Math.log(2d);

        return res;
    }

    /**
     * Variance of the geometric distribution value of level 'k' for 'n' total counts (numerically calculated).
     *
     * @param n the total number of counts
     * @param k The level number.
     * @return The sigma (variance).
     */
    static double sigma(long n, long k) {
        final double mu = mu(n, k);

        double sigmaSquaredSum = 0;

        for (long v = 0; v <= n; v++) {
            double abs_mu_v = Math.abs(mu - v);

            // Skip zero term in sum:
            if (abs_mu_v >= epsilon) {
                double ln_term = ln_pv(n, k, v);

                ln_term += 2 * Math.log(Math.abs(mu - v));

                final double term = Math.exp(ln_term);

                if (v > mu + 1 && Math.abs(term) <= epsilon)
                    break; // this term is too small, and further terms will be even smaller.

                sigmaSquaredSum += term;
            }
        }

        return Math.sqrt(sigmaSquaredSum);
    }

    /**
     * Real number square.
     *
     * @param x The argument.
     * @return The squared argument.
     */
    static double square(double x) {
        return x * x;
    }

    /**
     * Efficiently calculates ln( n!/((n-k)! * k!) ).
     *
     * @param n The n.
     * @param k The k.
     * @return ln( n!/((n-k)! * k!) ).
     */
    @SuppressWarnings("JavaDoc")
    static double ln_binomial(long n, long k) {
        final long max, min;

        if (k > n - k) {
            max = k;
            min = n - k;
        }
        else {
            max = n - k;
            min = k;
        }

        return ln_facFraction(n, max) - ln_factorial(min);
    }

    /**
     * Calculates ln(a!/b!)
     *
     * @param a The numerator.
     * @param b The denominator.
     * @return ln(a!/b!)
     */
    @SuppressWarnings("JavaDoc")
    static double ln_facFraction(long a, long b) {
        assert (a == 0 && b == 1) || a >= b : "a = " + a + ", b = " + b;

        double sum = 0d;

        if (a == b)
            return sum;

        for (long i=b+1; i<=a; i++)
            sum += Math.log(i);

        return sum;
    }

    /**
     * Calculates logarithm of a factorial.
     *
     * @param x The argument.
     * @return ln(x!)
     */
    @SuppressWarnings("JavaDoc")
    static double ln_factorial(long x) {
        return ln_facFraction(x, 1);
    }

    /**
     * The 0-based geometric distribution density:
     * E(n, k) = mu(n, k) = n / (2 ^(k + 1)).
     *
     * @param n The total counts.
     * @param k The level number.
     * @return The expected average for the level 'k' of 'n' total counts.
     */
    static double mu(long n, long k) {
        return n / pow2(k + 1);
    }

    /**
     * Utility function, power of 2.
     *
     * @param pow The power.
     * @return 2^pow
     */
    static double pow2(long pow) {
        if (pow >= 0)
            return 1L << pow;
        else
            return 1d / pow2(-pow);
    }

    /**
     * @throws Exception On error.
     */
    public void testMapSimple() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

//        mem.listen(new GridOffHeapEventListener() {
//            @Override public void onEvent(GridOffHeapEvent evt) {
//                if (evt == GridOffHeapEvent.ALLOCATE)
//                    U.dumpStack();
//            }
//        });

        Random rnd = new Random();

        int mapSize = 16 << rnd.nextInt(6);

        HadoopJobInfo job = new JobInfo();

        HadoopTaskContext taskCtx = new TaskContext();

        HadoopMultimap m = new HadoopSkipList(job, mem);

        HadoopMultimap.Adder a = m.startAdding(taskCtx);

        Multimap<Integer, Integer> mm = ArrayListMultimap.create();
        Multimap<Integer, Integer> vis = ArrayListMultimap.create();

        for (int i = 0, vals = 4 * mapSize + rnd.nextInt(25); i < vals; i++) {
            int key = rnd.nextInt(mapSize);
            int val = rnd.nextInt();

            a.write(new IntWritable(key), new IntWritable(val));
            mm.put(key, val);

            X.println("k: " + key + " v: " + val);

            a.close();

            check(m, mm, vis, taskCtx);

            a = m.startAdding(taskCtx);
        }

//        a.add(new IntWritable(10), new IntWritable(2));
//        mm.put(10, 2);
//        check(m, mm);

        a.close();

        X.println("Alloc: " + mem.allocatedSize());

        m.close();

        assertEquals(0, mem.allocatedSize());
    }

    /**
     * Check.
     * @param m The multimap.
     * @param mm The multimap storing expectations.
     * @param vis The multimap to store visitor results.
     * @param taskCtx The task context.
     * @throws Exception On error.
     */
    private void check(HadoopMultimap m, Multimap<Integer, Integer> mm, final Multimap<Integer, Integer> vis,
        HadoopTaskContext taskCtx)
        throws Exception {
        final HadoopTaskInput in = m.input(taskCtx);

        Map<Integer, Collection<Integer>> mmm = mm.asMap();

        int keys = 0;

        int prevKey = Integer.MIN_VALUE;

        while (in.next()) {
            keys++;

            IntWritable k = (IntWritable)in.key();

            assertNotNull(k);

            assertTrue(k.get() > prevKey);

            prevKey = k.get();

            Deque<Integer> vs = new LinkedList<>();

            Iterator<?> it = in.values();

            while (it.hasNext())
                vs.addFirst(((IntWritable) it.next()).get());

            Collection<Integer> exp = mmm.get(k.get());

            assertEquals(exp, vs);
        }

        assertEquals(mmm.size(), keys);

//!        assertEquals(m.keys(), keys);

        // Check visitor.

        final byte[] buf = new byte[4];

        final GridDataInput dataInput = new GridUnsafeDataInput();

        m.visit(false, new HadoopMultimap.Visitor() {
            /** */
            IntWritable key = new IntWritable();

            /** */
            IntWritable val = new IntWritable();

            @Override public void onKey(long keyPtr, int keySize) {
                read(keyPtr, keySize, key);
            }

            @Override public void onValue(long valPtr, int valSize) {
                read(valPtr, valSize, val);

                vis.put(key.get(), val.get());
            }

            private void read(long ptr, int size, Writable w) {
                assert size == 4 : size;

                GridUnsafe.copyOffheapHeap(ptr, buf, GridUnsafe.BYTE_ARR_OFF, size);

                dataInput.bytes(buf, size);

                try {
                    w.readFields(dataInput);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

//        X.println("vis: " + vis);

        assertEquals(mm, vis);

        in.close();
    }

    /**
     * @throws Exception if failed.
     */
    public void testMultiThreaded() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

        X.println("___ Started");

        Random rnd = new GridRandom();

        for (int i = 0; i < 20; i++) {
            HadoopJobInfo job = new JobInfo();

            final HadoopTaskContext taskCtx = new TaskContext();

            final HadoopMultimap m = new HadoopSkipList(job, mem);

            final ConcurrentMap<Integer, Collection<Integer>> mm = new ConcurrentHashMap<>();

            X.println("___ MT");

            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    X.println("___ TH in");

                    Random rnd = new GridRandom();

                    IntWritable key = new IntWritable();
                    IntWritable val = new IntWritable();

                    HadoopMultimap.Adder a = m.startAdding(taskCtx);

                    for (int i = 0; i < 50000; i++) {
                        int k = rnd.nextInt(32000);
                        int v = rnd.nextInt();

                        key.set(k);
                        val.set(v);

                        a.write(key, val);

                        Collection<Integer> list = mm.get(k);

                        if (list == null) {
                            list = new ConcurrentLinkedQueue<>();

                            Collection<Integer> old = mm.putIfAbsent(k, list);

                            if (old != null)
                                list = old;
                        }

                        list.add(v);
                    }

                    a.close();

                    X.println("___ TH out");

                    return null;
                }
            }, 3 + rnd.nextInt(27));

            HadoopTaskInput in = m.input(taskCtx);

            int prevKey = Integer.MIN_VALUE;

            while (in.next()) {
                IntWritable key = (IntWritable)in.key();

                assertTrue(key.get() > prevKey);

                prevKey = key.get();

                Iterator<?> valsIter = in.values();

                Collection<Integer> vals = mm.remove(key.get());

                assertNotNull(vals);

                while (valsIter.hasNext()) {
                    IntWritable val = (IntWritable) valsIter.next();

                    assertTrue(vals.remove(val.get()));
                }

                assertTrue(vals.isEmpty());
            }

            in.close();
            m.close();

            assertEquals(0, mem.allocatedSize());
        }
    }

    /**
     * Utility class (not thread safe) to compute statistics of a value.
     *
     * @param <T> The value type.
     */
    static class ValueStatistics<T extends Number> {
        /** */
        private final List<T> valList = new ArrayList<>();

        /** */
        private boolean ready;

        /** */
        private long cnt;

        /** */
        private double average;

        /** */
        private double standardDeviation;

        /**
         * Adds one more value.
         *
         * @param val The value.
         */
        public void addValue(T val) {
            valList.add(val);
        }

        /**
         * Claculates the statistics.
         */
        public void calculate() {
            checkNotReady();

            cnt = valList.size();

            if (cnt < 2)
                throw new IllegalStateException("Not enough values to calculate statistics: " + cnt);

            double sum = 0;

            for (T t : valList)
                sum += t.doubleValue();

            average = sum / cnt;

            double sqrSum = 0;

            for (T t : valList)
                sqrSum += square(t.doubleValue() - average);

            standardDeviation = Math.sqrt(sqrSum / (cnt - 1));

            ready = true;

            checkReady();
        }

        /**
         * Checks if the statistics is ready.
         */
        private void checkReady() {
            if (!ready)
                throw new IllegalStateException("Statistics not yet calculated.");
        }

        /**
         * Checks if the statistics is *not* ready.
         */
        private void checkNotReady() {
            if (ready)
                throw new IllegalStateException("Statistics already calculated.");
        }

        /**
         * @return the average.
         */
        public double getAverage() {
            checkReady();

            return average;
        }

        /**
         * @return The deviation.
         */
        public double getStandardDeviation() {
            checkReady();

            return standardDeviation;
        }

        /**
         * @return The total number of values.
         */
        public long getCount() {
            checkReady();

            return cnt;
        }
    }
}
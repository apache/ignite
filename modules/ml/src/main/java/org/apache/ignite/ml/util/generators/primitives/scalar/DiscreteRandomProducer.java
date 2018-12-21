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

package org.apache.ignite.ml.util.generators.primitives.scalar;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;

/**
 * Pseudorandom producer generating values from user provided discrete distribution.
 */
public class DiscreteRandomProducer extends RandomProducerWithGenerator {
    /** */
    private double EPS = 1e-5;

    /** Probabilities. */
    private final double[] probs;

    /** Random variable values. */
    private final int[] ids;

    /**
     * Creates an instance of DiscreteRandomProducer.
     *
     * @param probs Discrete distribution probabilities.
     */
    public DiscreteRandomProducer(double... probs) {
        this(System.currentTimeMillis(), probs);
    }

    /**
     * Creates an instance of DiscreteRandomProducer.
     *
     * @param seed Seed.
     * @param probs Discrete distribution probabilities.
     */
    public DiscreteRandomProducer(long seed, double... probs) {
        super(seed);

        boolean allElementsAreGEZero = Arrays.stream(probs).allMatch(p -> p >= 0.0);
        boolean sumOfProbsEqOne = Math.abs(Arrays.stream(probs).sum() - 1.0) < EPS;
        A.ensure(allElementsAreGEZero, "all elements should be great or equals 0.0");
        A.ensure(sumOfProbsEqOne, "sum of probs should equal 1.0");

        this.probs = Arrays.copyOf(probs, probs.length);
        this.ids = IntStream.range(0, probs.length).toArray();
        sort(this.probs, ids, 0, probs.length - 1);

        int i = 0;
        int j = probs.length - 1;
        while (i < j) {
            double temp = this.probs[i];
            this.probs[i] = this.probs[j];
            this.probs[j] = temp;

            int idxTmp = this.ids[i];
            this.ids[i] = this.ids[j];
            this.ids[j] = idxTmp;

            i++;
            j--;
        }

        for (i = 1; i < this.probs.length; i++)
            this.probs[i] += this.probs[i - 1];
    }

    /**
     * Creates a producer of random values from uniform discrete distribution.
     *
     * @param numberOfValues Number of distinct values.
     * @return Producer.
     */
    public static DiscreteRandomProducer uniform(int numberOfValues) {
        return uniform(numberOfValues, System.currentTimeMillis());
    }

    /**
     * Creates a producer of random values from uniform discrete distribution.
     *
     * @param numberOfValues Number of distinct values.
     * @param seed Seed.
     * @return Producer.
     */
    public static DiscreteRandomProducer uniform(int numberOfValues, long seed) {
        return new DiscreteRandomProducer(seed, IntStream.range(0, numberOfValues)
            .mapToDouble(x -> 1.0 / numberOfValues)
            .toArray());
    }

    /**
     * Generates pseudorandom discrete distribution.
     *
     * @param numberOfValues Number of distinct values of pseudorandom variable.
     * @return Probabilities array.
     */
    public static double[] randomDistribution(int numberOfValues) {
        return randomDistribution(numberOfValues, System.currentTimeMillis());
    }

    /**
     * Generates pseudorandom discrete distribution.
     *
     * @param numberOfValues Number of distinct values of pseudorandom variable.
     * @param seed Seed.
     * @return Probabilities array.
     */
    public static double[] randomDistribution(int numberOfValues, long seed) {
        A.ensure(numberOfValues > 0, "numberOfValues > 0");

        Random random = new Random(seed);
        long[] rnd = IntStream.range(0, numberOfValues).mapToLong(i -> random.nextInt(Integer.MAX_VALUE))
            .limit(numberOfValues)
            .toArray();
        long sum = Arrays.stream(rnd).sum();

        double[] result = new double[numberOfValues];
        for (int i = 0; i < result.length; i++)
            result[i] = rnd[i] / Math.max(1.0, sum);

        return result;
    }

    /** {@inheritDoc} */
    @Override public Double get() {
        double p = generator().nextDouble();
        for (int i = 0; i < probs.length; i++) {
            if (probs[i] > p)
                return (double)ids[i];
        }

        return (double)ids[probs.length - 1];
    }

    /**
     * @return Value of preudorandom discrete variable.
     */
    public int getInt() {
        return get().intValue();
    }

    /**
     * @return Count of distinct values of distribution.
     */
    public int size() {
        return probs.length;
    }

    /**
     * Sort of probabilities values and corresponded indicies.
     *
     * @param probs Probabilities.
     * @param idx Random variable values.
     * @param from From.
     * @param to To.
     */
    private void sort(double[] probs, int[] idx, int from, int to) {
        if (from < to) {
            double pivot = probs[(from + to) / 2];

            int i = from, j = to;

            while (i <= j) {
                while (probs[i] < pivot)
                    i++;
                while (probs[j] > pivot)
                    j--;

                if (i <= j) {
                    double tmpFeature = probs[i];
                    probs[i] = probs[j];
                    probs[j] = tmpFeature;

                    int tmpLb = idx[i];
                    idx[i] = idx[j];
                    idx[j] = tmpLb;

                    i++;
                    j--;
                }
            }

            sort(probs, idx, from, j);
            sort(probs, idx, i, to);
        }
    }
}

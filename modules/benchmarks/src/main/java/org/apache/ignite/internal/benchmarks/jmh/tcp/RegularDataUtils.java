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

package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.nio.ByteBuffer;
import java.util.Random;

/** Generates regular data with statistics close to native texts. */
final class RegularDataUtils {
    /**
     * Generates regular data with statistics close to native texts.
     *
     * @param size Size of generating data.
     * @param n Number of words in language.
     * @return Regular data.
     */
    static byte[] generateRegularData(int size, int n) {
        ByteBuffer buf = ByteBuffer.allocate(size);

        Language language = generateLanguage(n);

        Random rnd = new Random(3L);

        while (buf.hasRemaining()) {
            byte[] word = language.words[getIndex(language.probabilities, rnd.nextDouble())];

            buf.put(word, 0, (buf.remaining() < word.length) ? buf.remaining() : word.length);
        }

        return buf.array();
    }

    /**
     * Poisson distribution.
     *
     * @param k Number of events is first parameter of poisson distribution.
     * @param l Lambda is second parameter of poisson distribution.
     * @return Probability.
     */
    private static double poisson(int k, double l) {
        double x = StrictMath.exp(-l);

        for (int i = 1; i < k; i++)
            x *= (l / i);

        return x;
    }

    /**
     * Distribution for Ziph's law.
     *
     * @param length Length of word.
     * @return Probability.
     */
    private static double ziph(int length) {
        return 1.0 / length;
    }

    /**
     * Convert weights to distribution in place.
     *
     * @param weights Weights.
     */
    private static void normalize(double... weights) {
        if (weights.length < 1)
            return;

        double sum = weights[0];

        for (int i = 1; i < weights.length; i++) {
            sum += weights[i];
            weights[i] += weights[i - 1];
        }

        for (int i = 0; i < weights.length; i++)
            weights[i] /= sum;
    }

    /**
     * Generates random lengths from poisson distribution.
     *
     * @param size Size of generating lengths.
     * @return Lengths of words.
     */
    private static int[] getLengths(int size) {
        double[] probabilities = new double[size];

        for (int i = 0; i < size; i++)
            probabilities[i] = poisson(i, StrictMath.log(size));

        normalize(probabilities);

        int[] lengths = new int[size];

        Random rnd = new Random(31L);

        for (int i = 0; i < size; i++)
            lengths[i] = 1 + getIndex(probabilities, rnd.nextDouble());

        return lengths;
    }

    /**
     * Help to get weighted random index.
     *
     * @param distribution Distribution.
     * @param x Random double.
     * @return Index.
     */
    private static int getIndex(double[] distribution, double x) {
        int i = 0;

        while (x > distribution[i])
            i++;

        return i;
    }

    /**
     * Generate {@link Language}.
     *
     * @param size Size of language.
     * @return Language.
     */
    private static Language generateLanguage(int size) {
        int[] lengths = getLengths(size);

        double[] probabilities = new double[size];

        for (int i = 0; i < size; i++)
            probabilities[i] = ziph(lengths[i]);

        normalize(probabilities);

        byte[][] words = new byte[size][];

        Random rnd = new Random(314L);

        for (int i = 0; i < size; i++) {
            byte[] word = new byte[lengths[i]];

            rnd.nextBytes(word);

            words[i] = word;
        }

        return new Language(probabilities, words);
    }

    /** Language with weighted words. */
    private static final class Language {
        /** */
        private final double[] probabilities;

        /** */
        private final byte[][] words;

        /**
         * @param probabilities Probabilities.
         * @param words Words.
         */
        private Language(double[] probabilities, byte[][] words) {
            this.probabilities = probabilities;
            this.words = words;
        }
    }
}

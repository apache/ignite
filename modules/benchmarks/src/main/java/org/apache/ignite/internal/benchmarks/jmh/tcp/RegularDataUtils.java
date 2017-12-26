package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.nio.ByteBuffer;
import java.util.Random;

/** */
final class RegularDataUtils {
    /**
     * Poisson distribution.
     */
    private static double poisson(int k, double a) {
        double x = StrictMath.exp(-a);

        for (int i = 1; i < k; i++)
            x *= (a / i);

        return x;
    }

    /**
     * Distribution for Ziph's law.
     */
    private static double ziph(int k) {
        return 1.0 / k;
    }

    /** Convert weights to distribution. */
    private static void normalize(double... x) {
        if (x.length < 1)
            return;

        double sum = x[0];

        for (int i = 1; i < x.length; i++) {
            sum += x[i];
            x[i] += x[i - 1];
        }

        for (int i = 0; i < x.length; i++)
            x[i] /= sum;
    }

    /** Return lengths of words. */
    private static int[] getLengths(int n) {
        double[] probabilities = new double[n];

        for (int i = 0; i < n; i++)
            probabilities[i] = poisson(i, StrictMath.log(n));

        normalize(probabilities);

        int[] lengths = new int[n];

        Random random = new Random(31L);

        for (int i = 0; i < n; i++)
            lengths[i] = 1 + getIndex(probabilities, random.nextDouble());

        return lengths;
    }

    /** Help to get weighted random index. */
    private static int getIndex(double[] ps, double x) {
        int i = 0;

        while (x > ps[i])
            i++;

        return i;
    }

    /** */
    private static Language generateLanguage(int n) {
        int[] lengths = getLengths(n);

        double[] probabilities = new double[n];

        for (int i = 0; i < n; i++)
            probabilities[i] = ziph(lengths[i]);

        normalize(probabilities);

        byte[][] words = new byte[n][];

        Random random = new Random(314L);

        for (int i = 0; i < n; i++) {
            byte[] word = new byte[lengths[i]];

            random.nextBytes(word);

            words[i] = word;
        }

        return new Language(lengths, probabilities, words);
    }

    /** */
    private static class Language {
        /** */
        private final int[] lengths;

        /** */
        private final double[] probabilities;

        /** */
        private final byte[][] words;

        /** */
        private Language(int[] lengths, double[] probabilities, byte[][] words) {
            this.lengths = lengths;
            this.probabilities = probabilities;
            this.words = words;
        }
    }

    /** Return regular data with statistics close to native texts. */
    static byte[] generateRegularData(int size, int n) {
        ByteBuffer buf = ByteBuffer.allocate(size);

        Language language = generateLanguage(n);

        Random random = new Random(3L);

        while (buf.hasRemaining()) {
            byte[] word = language.words[getIndex(language.probabilities, random.nextDouble())];

            buf.put(word, 0, (buf.remaining() < word.length) ? buf.remaining() : word.length);
        }

        return buf.array();
    }
}

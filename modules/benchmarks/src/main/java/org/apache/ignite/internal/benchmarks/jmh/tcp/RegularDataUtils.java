package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.nio.ByteBuffer;
import java.util.Random;

/** */
public class RegularDataUtils {
    /**
     * Poisson distribution.
     */
    static double p(int k, double a) {
        double x = Math.exp(-a);

        for (int i = 1; i < k; i++)
            x *= (a/i);

        return x;
    }

    /**
     * Distribution for Ziph's law.
     */
    static double z(int k) {
        return 1.0/k;
    }

    static void normalize(double[] x) {
        if (x.length < 1) return;

        double sum = x[0];

        for (int i = 1; i < x.length; i++) {
            sum += x[i];
            x[i] += x[i-1];
        }

        for (int i = 0; i < x.length; i++) {
            x[i] = x[i] / sum;
        }
    }

    /**
     * Return lengths of words.
     */
    static  int[] getLengths(int n) {
        double[] probabilities = new double[n];

        for (int i = 0; i < n; i++)
            probabilities[i] = p(i, Math.log(n));

        normalize(probabilities);

        // длины слов, которые мы сгенерируем
        int[] lengths = new int[n];

        Random random = new Random(31);

        for (int i = 0; i < n; i++)
            lengths[i] = 1+getIndex(probabilities, random.nextDouble());

        return lengths;
    }


    /** */
    static int getIndex(double[] ps, double x) {
        int i = 0;

        while (x > ps[i])
            i++;

        return i;
    }

    /** */
    static Words getWords(int n) {
        int[] lengths = getLengths(n);

        double[] probabilities = new double[n];

        for (int i = 0; i < n; i++)
            probabilities[i] = z(lengths[i]);

        normalize(probabilities);

        byte[][] words = new byte[n][];

        Random random = new Random(314);

        for (int i = 0; i < n; i++) {
            byte[] word = new byte[lengths[i]];

            random.nextBytes(word);

            words[i] = word;
        }

        return new Words(lengths, probabilities, words);
    }

    /** */
    static class Words {
        /** */
        final int[] lengths;

        /** */
        final double[] probabilities;

        /** */
        final byte[][] words;

        /** */
        Words(int[] lengths, double[] probabilities, byte[][] words) {
            this.lengths = lengths;
            this.probabilities = probabilities;
            this.words = words;
        }
    }

    /** */
    static byte[] generateRegularData(int size, int n) {
        ByteBuffer buffer = ByteBuffer.allocate(size);

        Words words = getWords(n);

        Random random = new Random(3);

        while (buffer.hasRemaining()) {
            byte[] word = words.words[getIndex(words.probabilities, random.nextDouble())];

            buffer.put(word, 0, (buffer.remaining() < word.length) ? buffer.remaining() : word.length);
        }

        return buffer.array();
    }
}

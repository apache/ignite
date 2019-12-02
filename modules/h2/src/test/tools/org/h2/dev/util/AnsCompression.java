/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * An ANS (Asymmetric Numeral Systems) compression tool.
 * It uses the range variant.
 */
public class AnsCompression {

    private static final long TOP = 1L << 24;
    private static final int SHIFT = 12;
    private static final int MASK = (1 << SHIFT) - 1;
    private static final long MAX = (TOP >> SHIFT) << 32;

    private AnsCompression() {
        // a utility class
    }

    /**
     * Count the frequencies of codes in the data, and increment the target
     * frequency table.
     *
     * @param freq the target frequency table
     * @param data the data
     */
    public static void countFrequencies(int[] freq, byte[] data) {
        for (byte x : data) {
            freq[x & 0xff]++;
        }
    }

    /**
     * Scale the frequencies to a new total. Frequencies of 0 are kept as 0;
     * larger frequencies result in at least 1.
     *
     * @param freq the (source and target) frequency table
     * @param total the target total (sum of all frequencies)
     */
    public static void scaleFrequencies(int[] freq, int total) {
        int len = freq.length, sum = 0;
        for (int x : freq) {
            sum += x;
        }
        // the list of: (error << 8) + index
        int[] errors = new int[len];
        int totalError = -total;
        for (int i = 0; i < len; i++) {
            int old = freq[i];
            if (old == 0) {
                continue;
            }
            int ideal = (int) (old * total * 256L / sum);
            // 1 too high so we can decrement if needed
            int x = 1 + ideal / 256;
            freq[i] = x;
            totalError += x;
            errors[i] = ((x * 256 - ideal) << 8) + i;
        }
        // we don't need to sort, we could just calculate
        // which one is the nth element - but sorting is simpler
        Arrays.sort(errors);
        if (totalError < 0) {
            // integer overflow
            throw new IllegalArgumentException();
        }
        while (totalError > 0) {
            for (int i = 0; totalError > 0 && i < len; i++) {
                int index = errors[i] & 0xff;
                if (freq[index] > 1) {
                    freq[index]--;
                    totalError--;
                }
            }
        }
    }

    /**
     * Generate the cumulative frequency table.
     *
     * @param freq the source frequency table
     * @return the cumulative table, with one entry more
     */
    static int[] generateCumulativeFrequencies(int[] freq) {
        int len = freq.length;
        int[] cumulativeFreq = new int[len + 1];
        for (int i = 0, x = 0; i < len; i++) {
            x += freq[i];
            cumulativeFreq[i + 1] = x;
        }
        return cumulativeFreq;
    }

    /**
     * Generate the frequency-to-code table.
     *
     * @param cumulativeFreq the cumulative frequency table
     * @return the result
     */
    private static byte[] generateFrequencyToCode(int[] cumulativeFreq) {
        byte[] freqToCode = new byte[1 << SHIFT];
        int x = 0;
        byte s = -1;
        for (int i : cumulativeFreq) {
            while (x < i) {
                freqToCode[x++] = s;
            }
            s++;
        }
        return freqToCode;
    }

    /**
     * Encode the data.
     *
     * @param freq the frequency table (will be scaled)
     * @param data the source data (uncompressed)
     * @return the compressed data
     */
    public static byte[] encode(int[] freq, byte[] data) {
        scaleFrequencies(freq, 1 << SHIFT);
        int[] cumulativeFreq = generateCumulativeFrequencies(freq);
        ByteBuffer buff = ByteBuffer.allocate(data.length * 2);
        buff = encode(data, freq, cumulativeFreq, buff);
        return Arrays.copyOfRange(buff.array(),
                buff.arrayOffset() + buff.position(), buff.arrayOffset() + buff.limit());
    }

    private static ByteBuffer encode(byte[] data, int[] freq,
            int[] cumulativeFreq, ByteBuffer buff) {
        long state = TOP;
        // encoding happens backwards
        int b = buff.limit();
        for (int p = data.length - 1; p >= 0; p--) {
            int x = data[p] & 0xff;
            int f = freq[x];
            while (state >= MAX * f) {
                b -= 4;
                buff.putInt(b, (int) state);
                state >>>= 32;
            }
            state = ((state / f) << SHIFT) + (state % f) + cumulativeFreq[x];
        }
        b -= 8;
        buff.putLong(b, state);
        buff.position(b);
        return buff.slice();
    }

    /**
     * Decode the data.
     *
     * @param freq the frequency table (will be scaled)
     * @param data the compressed data
     * @param length the target length
     * @return the uncompressed result
     */
    public static byte[] decode(int[] freq, byte[] data, int length) {
        scaleFrequencies(freq, 1 << SHIFT);
        int[] cumulativeFreq = generateCumulativeFrequencies(freq);
        byte[] freqToCode = generateFrequencyToCode(cumulativeFreq);
        byte[] out = new byte[length];
        decode(data, freq, cumulativeFreq, freqToCode, out);
        return out;
    }

    private static void decode(byte[] data, int[] freq, int[] cumulativeFreq,
            byte[] freqToCode, byte[] out) {
        ByteBuffer buff = ByteBuffer.wrap(data);
        long state = buff.getLong();
        for (int i = 0, size = out.length; i < size; i++) {
            int x = (int) state & MASK;
            int c = freqToCode[x] & 0xff;
            out[i] = (byte) c;
            state = (freq[c] * (state >> SHIFT)) + x - cumulativeFreq[c];
            while (state < TOP) {
                state = (state << 32) | (buff.getInt() & 0xffffffffL);
            }
        }
    }

}

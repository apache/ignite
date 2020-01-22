/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

/**
 * The random data generator used for BenchC.
 */
public class BenchCRandom {

    private final Random random = new Random(10);

    /**
     * Get a non-uniform random integer value between min and max.
     *
     * @param a the bit mask
     * @param min the minimum value
     * @param max the maximum value
     * @return the random value
     */
    int getNonUniform(int a, int min, int max) {
        int c = 0;
        return (((getInt(0, a) | getInt(min, max)) + c) % (max - min + 1))
                + min;
    }

    /**
     * Get a random integer value between min and max.
     *
     * @param min the minimum value
     * @param max the maximum value
     * @return the random value
     */
    int getInt(int min, int max) {
        return max <= min ? min : (random.nextInt(max - min) + min);
    }

    /**
     * Generate a boolean array with this many items set to true (randomly
     * distributed).
     *
     * @param length the size of the array
     * @param trueCount the number of true elements
     * @return the boolean array
     */
    boolean[] getBoolean(int length, int trueCount) {
        boolean[] data = new boolean[length];
        for (int i = 0, pos; i < trueCount; i++) {
            do {
                pos = getInt(0, length);
            } while (data[pos]);
            data[pos] = true;
        }
        return data;
    }

    /**
     * Replace a random part of the string with another text.
     *
     * @param text the original text
     * @param replacement the replacement
     * @return the patched string
     */
    String replace(String text, String replacement) {
        int pos = getInt(0, text.length() - replacement.length());
        StringBuilder buffer = new StringBuilder(text);
        buffer.replace(pos, pos + 7, replacement);
        return buffer.toString();
    }

    /**
     * Get a random number string.
     *
     * @param min the minimum value
     * @param max the maximum value
     * @return the number string
     */
    String getNumberString(int min, int max) {
        int len = getInt(min, max);
        char[] buff = new char[len];
        for (int i = 0; i < len; i++) {
            buff[i] = (char) getInt('0', '9');
        }
        return new String(buff);
    }

    /**
     * Get random address data.
     *
     * @return the address
     */
    String[] getAddress() {
        String str1 = getString(10, 20);
        String str2 = getString(10, 20);
        String city = getString(10, 20);
        String state = getString(2);
        String zip = getNumberString(9, 9);
        return new String[] { str1, str2, city, state, zip };
    }

    /**
     * Get a random string.
     *
     * @param min the minimum size
     * @param max the maximum size
     * @return the string
     */
    String getString(int min, int max) {
        return getString(getInt(min, max));
    }

    /**
     * Get a random string.
     *
     * @param len the size
     * @return the string
     */
    String getString(int len) {
        char[] buff = new char[len];
        for (int i = 0; i < len; i++) {
            buff[i] = (char) getInt('A', 'Z');
        }
        return new String(buff);
    }

    /**
     * Generate a random permutation if the values 0 .. length.
     *
     * @param length the number of elements
     * @return the random permutation
     */
    int[] getPermutation(int length) {
        int[] data = new int[length];
        for (int i = 0; i < length; i++) {
            data[i] = i;
        }
        for (int i = 0; i < length; i++) {
            int j = getInt(0, length);
            int temp = data[i];
            data[i] = data[j];
            data[j] = temp;
        }
        return data;
    }

    /**
     * Create a big decimal value.
     *
     * @param value the value
     * @param scale the scale
     * @return the big decimal object
     */
    BigDecimal getBigDecimal(int value, int scale) {
        return new BigDecimal(new BigInteger(String.valueOf(value)), scale);
    }

    /**
     * Generate a last name composed of three elements
     *
     * @param i the last name index
     * @return the name
     */
    String getLastname(int i) {
        String[] n = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI",
                "CALLY", "ATION", "EING" };
        StringBuilder buff = new StringBuilder();
        buff.append(n[i / 100]);
        buff.append(n[(i / 10) % 10]);
        buff.append(n[i % 10]);
        return buff.toString();
    }

}

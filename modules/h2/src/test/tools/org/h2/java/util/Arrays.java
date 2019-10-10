/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.util;

/**
 * An simple implementation of java.util.Arrays
 */
public class Arrays {

    /**
     * Fill an array with the given value.
     *
     * @param array the array
     * @param x the value
     */
    public static void fill(char[] array, char x) {
        for (int i = 0, size = array.length; i < size; i++) {
            array[i] = x;
        }
    }

    /**
     * Fill an array with the given value.
     *
     * @param array the array
     * @param x the value
     */
    public static void fill(byte[] array, byte x) {
        for (int i = 0; i < array.length; i++) {
            array[i] = x;
        }
    }

    /**
     * Fill an array with the given value.
     *
     * @param array the array
     * @param x the value
     */
    public static void fill(int[] array, int x) {
        for (int i = 0; i < array.length; i++) {
            array[i] = x;
        }
    }


    /**
     * Fill an array with the given value.
     *
     * @param array the array
     * @param x the value
     */
    public static void fillByte(byte[] array, byte x) {
        for (int i = 0; i < array.length; i++) {
            array[i] = x;
        }
    }

    /**
     * Fill an array with the given value.
     *
     * @param array the array
     * @param x the value
     */
    public static void fillInt(int[] array, int x) {
        for (int i = 0; i < array.length; i++) {
            array[i] = x;
        }
    }

}

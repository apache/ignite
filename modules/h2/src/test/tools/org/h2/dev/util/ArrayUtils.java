/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Comparator;

/**
 * Array utility methods.
 */
public class ArrayUtils {

    /**
     * Sort an array using binary insertion sort
     *
     * @param <T> the type
     * @param d the data
     * @param left the index of the leftmost element
     * @param right the index of the rightmost element
     * @param comp the comparison class
     */
    public static <T> void binaryInsertionSort(T[] d, int left, int right,
            Comparator<T> comp) {
        for (int i = left + 1; i <= right; i++) {
            T t = d[i];
            int l = left;
            for (int r = i; l < r;) {
                int m = (l + r) >>> 1;
                if (comp.compare(t, d[m]) >= 0) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            for (int n = i - l; n > 0;) {
                d[l + n--] = d[l + n];
            }
            d[l] = t;
        }
    }

    /**
     * Sort an array using insertion sort
     *
     * @param <T> the type
     * @param d the data
     * @param left the index of the leftmost element
     * @param right the index of the rightmost element
     * @param comp the comparison class
     */
    public static <T> void insertionSort(T[] d, int left, int right,
            Comparator<T> comp) {
        for (int i = left + 1, j; i <= right; i++) {
            T t = d[i];
            for (j = i - 1; j >= left && comp.compare(d[j], t) > 0; j--) {
                d[j + 1] = d[j];
            }
            d[j + 1] = t;
        }
    }


}

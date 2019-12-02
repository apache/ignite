/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.sort;

import java.util.Comparator;

/**
 * A stable merge sort implementation that uses at most O(log(n)) memory
 * and O(n*log(n)*log(n)) time.
 *
 * @param <T> the element type
 */
public class InPlaceStableMergeSort<T> {

    /**
     * The minimum size of the temporary array. It is used to speed up sorting
     * small blocks.
     */
    private static final int TEMP_SIZE = 1024;

    /**
     * Blocks smaller than this number are sorted using binary insertion sort.
     * This usually speeds up sorting.
     */
    private static final int INSERTION_SORT_SIZE = 16;

    /**
     * The data array to sort.
     */
    private T[] data;

    /**
     * The comparator.
     */
    private Comparator<T> comp;

    /**
     * The temporary array.
     */
    private T[] temp;

    /**
     * Sort an array using the given comparator.
     *
     * @param data the data array to sort
     * @param comp the comparator
     */
    public static <T> void sort(T[] data, Comparator<T> comp) {
        new InPlaceStableMergeSort<T>().sortArray(data, comp);
    }

    /**
     * Sort an array using the given comparator.
     *
     * @param d the data array to sort
     * @param c the comparator
     */
    public void sortArray(T[] d, Comparator<T> c) {
        this.data = d;
        this.comp = c;
        int len = Math.max((int) (100 * Math.log(d.length)), TEMP_SIZE);
        len = Math.min(d.length, len);
        @SuppressWarnings("unchecked")
        T[] t = (T[]) new Object[len];
        this.temp = t;
        mergeSort(0, d.length - 1);
    }

    /**
     * Sort a block recursively using merge sort.
     *
     * @param from the index of the first entry to sort
     * @param to the index of the last entry to sort
     */
    void mergeSort(int from, int to) {
        if (to - from < INSERTION_SORT_SIZE) {
            binaryInsertionSort(from, to);
            return;
        }
        int m = (from + to) >>> 1;
        mergeSort(from, m);
        mergeSort(m + 1, to);
        merge(from, m + 1, to);
    }

    /**
     * Sort a block using the binary insertion sort algorithm.
     *
     * @param from the index of the first entry to sort
     * @param to the index of the last entry to sort
     */
    private void binaryInsertionSort(int from, int to) {
        for (int i = from + 1; i <= to; i++) {
            T x = data[i];
            int ins = binarySearch(x, from, i - 1);
            for (int j = i - 1; j >= ins; j--) {
                data[j + 1] = data[j];
            }
            data[ins] = x;
        }
    }

    /**
     * Find the index of the element that is larger than x.
     *
     * @param x the element to search
     * @param from the index of the first entry
     * @param to the index of the last entry
     * @return the position
     */
    private int binarySearch(T x, int from, int to) {
        while (from <= to) {
            int m = (from + to) >>> 1;
            if (comp.compare(x, data[m]) >= 0) {
                from = m + 1;
            } else {
                to = m - 1;
            }
        }
        return from;
    }

    /**
     * Merge two arrays.
     *
     * @param from the start of the first range
     * @param second start of the second range
     * @param to the last element of the second range
     */
    private void merge(int from, int second, int to) {
        int len1 = second - from, len2 = to - second + 1;
        if (len1 == 0 || len2 == 0) {
            return;
        }
        if (len1 + len2 == 2) {
            if (comp.compare(data[second], data[from]) < 0) {
                swap(data, second, from);
            }
            return;
        }
        if (len1 <= temp.length) {
            System.arraycopy(data, from, temp, 0, len1);
            mergeSmall(data, from, temp, 0, len1 - 1, data, second, to);
            return;
        } else if (len2 <= temp.length) {
            System.arraycopy(data, second, temp, 0, len2);
            System.arraycopy(data, from, data, to - len1 + 1, len1);
            mergeSmall(data, from, data, to - len1 + 1, to, temp, 0, len2 - 1);
            return;
        }
        mergeBig(from, second, to);
    }

    /**
     * Merge two (large) arrays. This is done recursively by merging the
     * beginning of both arrays, and then the end of both arrays.
     *
     * @param from the start of the first range
     * @param second start of the second range
     * @param to the last element of the second range
     */
    private void mergeBig(int from, int second, int to) {
        int len1 = second - from, len2 = to - second + 1;
        int firstCut, secondCut, newSecond;
        if (len1 > len2) {
            firstCut = from + len1 / 2;
            secondCut = findLower(data[firstCut], second, to);
            int len = secondCut - second;
            newSecond = firstCut + len;
        } else {
            int len = len2 / 2;
            secondCut = second + len;
            firstCut = findUpper(data[secondCut], from, second - 1);
            newSecond = firstCut + len;
        }
        swapBlocks(firstCut, second, secondCut - 1);
        merge(from, firstCut, newSecond - 1);
        merge(newSecond, secondCut, to);
    }

    /**
     * Merge two (small) arrays using the temporary array. This is done to speed
     * up merging.
     *
     * @param target the target array
     * @param pos the position of the first element in the target array
     * @param s1 the first source array
     * @param from1 the index of the first element in the first source array
     * @param to1 the index of the last element in the first source array
     * @param s2 the second source array
     * @param from2 the index of the first element in the second source array
     * @param to2 the index of the last element in the second source array
     */
    private void mergeSmall(T[] target, int pos, T[] s1, int from1, int to1,
            T[] s2, int from2, int to2) {
        T x1 = s1[from1], x2 = s2[from2];
        while (true) {
            if (comp.compare(x1, x2) <= 0) {
                target[pos++] = x1;
                if (++from1 > to1) {
                    System.arraycopy(s2, from2, target, pos, to2 - from2 + 1);
                    break;
                }
                x1 = s1[from1];
            } else {
                target[pos++] = x2;
                if (++from2 > to2) {
                    System.arraycopy(s1, from1, target, pos, to1 - from1 + 1);
                    break;
                }
                x2 = s2[from2];
            }
        }
    }

    /**
     * Find the largest element in the sorted array that is smaller than x.
     *
     * @param x the element to search
     * @param from the index of the first entry
     * @param to the index of the last entry
     * @return the index of the resulting element
     */
    private int findLower(T x, int from, int to) {
        int len = to - from + 1, half;
        while (len > 0) {
            half = len / 2;
            int m = from + half;
            if (comp.compare(data[m], x) < 0) {
                from = m + 1;
                len = len - half - 1;
            } else {
                len = half;
            }
        }
        return from;
    }

    /**
     * Find the smallest element in the sorted array that is larger than or
     * equal to x.
     *
     * @param x the element to search
     * @param from the index of the first entry
     * @param to the index of the last entry
     * @return the index of the resulting element
     */
    private int findUpper(T x, int from, int to) {
        int len = to - from + 1, half;
        while (len > 0) {
            half = len / 2;
            int m = from + half;
            if (comp.compare(data[m], x) <= 0) {
                from = m + 1;
                len = len - half - 1;
            } else {
                len = half;
            }
        }
        return from;
    }

    /**
     * Swap the elements of two blocks in the data array. Both blocks are next
     * to each other (the second block starts just after the first block ends).
     *
     * @param from the index of the first element in the first block
     * @param second the index of the first element in the second block
     * @param to the index of the last element in the second block
     */
    private void swapBlocks(int from, int second, int to) {
        int len1 = second - from, len2 = to - second + 1;
        if (len1 == 0 || len2 == 0) {
            return;
        }
        if (len1 < temp.length) {
            System.arraycopy(data, from, temp, 0, len1);
            System.arraycopy(data, second, data, from, len2);
            System.arraycopy(temp, 0, data, from + len2, len1);
            return;
        } else if (len2 < temp.length) {
            System.arraycopy(data, second, temp, 0, len2);
            System.arraycopy(data, from, data, from + len2, len1);
            System.arraycopy(temp, 0, data, from, len2);
            return;
        }
        reverseBlock(from, second - 1);
        reverseBlock(second, to);
        reverseBlock(from, to);
    }

    /**
     * Reverse all elements in a block.
     *
     * @param from the index of the first element
     * @param to the index of the last element
     */
    private void reverseBlock(int from, int to) {
        while (from < to) {
            T old = data[from];
            data[from++] = data[to];
            data[to--] = old;
        }
    }

    /**
     * Swap two elements in the array.
     *
     * @param d the array
     * @param a the index of the first element
     * @param b the index of the second element
     */
    private void swap(T[] d, int a, int b) {
        T t = d[a];
        d[a] = d[b];
        d[b] = t;
    }

}
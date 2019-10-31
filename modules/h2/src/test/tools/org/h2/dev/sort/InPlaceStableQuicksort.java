/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.sort;

import java.util.Comparator;

/**
 * A stable quicksort implementation that uses O(log(n)) memory. It normally
 * runs in O(n*log(n)*log(n)), but at most in O(n^2).
 *
 * @param <T> the element type
 */
public class InPlaceStableQuicksort<T> {

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
        new InPlaceStableQuicksort<T>().sortArray(data, comp);
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
        quicksort(0, d.length - 1);
    }

    /**
     * Sort a block using the quicksort algorithm.
     *
     * @param from the index of the first entry to sort
     * @param to the index of the last entry to sort
     */
    private void quicksort(int from, int to) {
        while (to > from) {
            if (to - from < INSERTION_SORT_SIZE) {
                binaryInsertionSort(from, to);
                return;
            }
            T pivot = selectPivot(from, to);
            int second = partition(pivot, from, to);
            if (second > to) {
                pivot = selectPivot(from, to);
                pivot = data[to];
                second = partition(pivot, from, to);
                if (second > to) {
                    second--;
                }
            }
            quicksort(from, second - 1);
            from = second;
        }
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
     * Move all elements that are bigger than the pivot to the end of the list,
     * and return the partitioning index. The partitioning index is the start
     * index of the range where all elements are larger than the pivot. If the
     * partitioning index is larger than the 'to' index, then all elements are
     * smaller or equal to the pivot.
     *
     * @param pivot the pivot
     * @param from the index of the first element
     * @param to the index of the last element
     * @return the the first element of the second partition
     */
    private int partition(T pivot, int from, int to) {
        if (to - from < temp.length) {
            return partitionSmall(pivot, from, to);
        }
        int m = (from + to + 1) / 2;
        int m1 = partition(pivot, from, m - 1);
        int m2 = partition(pivot, m, to);
        swapBlocks(m1, m, m2 - 1);
        return m1 + m2 - m;
    }

    /**
     * Partition a small block using the temporary array. This will speed up
     * partitioning.
     *
     * @param pivot the pivot
     * @param from the index of the first element
     * @param to the index of the last element
     * @return the the first element of the second partition
     */
    private int partitionSmall(T pivot, int from, int to) {
        int tempIndex = 0, dataIndex = from;
        for (int i = from; i <= to; i++) {
            T x = data[i];
            if (comp.compare(x, pivot) <= 0) {
                if (tempIndex > 0) {
                    data[dataIndex] = x;
                }
                dataIndex++;
            } else {
                temp[tempIndex++] = x;
            }
        }
        if (tempIndex > 0) {
            System.arraycopy(temp, 0, data, dataIndex, tempIndex);
        }
        return dataIndex;
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
     * Select a pivot. To ensure a good pivot is select, the median element of a
     * sample of the data is calculated.
     *
     * @param from the index of the first element
     * @param to the index of the last element
     * @return the pivot
     */
    private T selectPivot(int from, int to) {
        int count = (int) (6 * Math.log10(to - from));
        count = Math.min(count, temp.length);
        int step = (to - from) / count;
        for (int i = from, j = 0; i < to; i += step, j++) {
            temp[j] = data[i];
        }
        T pivot = select(temp, 0, count - 1, count / 2);
        return pivot;
    }

    /**
     * Select the specified element.
     *
     * @param d the array
     * @param from the index of the first element
     * @param to the index of the last element
     * @param k which element to return (1 means the lowest)
     * @return the specified element
     */
    private T select(T[] d, int from, int to, int k) {
        while (true) {
            int pivotIndex = (to + from) >>> 1;
            int pivotNewIndex = selectPartition(d, from, to, pivotIndex);
            int pivotDist = pivotNewIndex - from + 1;
            if (pivotDist == k) {
                return d[pivotNewIndex];
            } else if (k < pivotDist) {
                to = pivotNewIndex - 1;
            } else {
                k = k - pivotDist;
                from = pivotNewIndex + 1;
            }
        }
    }

    /**
     * Partition the elements to select an element.
     *
     * @param d the array
     * @param from the index of the first element
     * @param to the index of the last element
     * @param pivotIndex the index of the pivot
     * @return the new index
     */
    private int selectPartition(T[] d, int from, int to, int pivotIndex) {
        T pivotValue = d[pivotIndex];
        swap(d, pivotIndex, to);
        int storeIndex = from;
        for (int i = from; i <= to; i++) {
            if (comp.compare(d[i], pivotValue) < 0) {
                swap(d, storeIndex, i);
                storeIndex++;
            }
        }
        swap(d, to, storeIndex);
        return storeIndex;
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

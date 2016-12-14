package org.apache.ignite.internal.processors.hadoop.impl.io;

/**
 * Special version of raw comparator allowing direct access to the underlying memory.
 */
public interface PartialOffheapRawComparatorEx<T> {
    /**
     * Perform compare.
     *
     * @param val1 First value.
     * @param val2Ptr Pointer to the second value data.
     * @param val2Len Length of the second value data.
     * @return Result.
     */
    int compare(T val1, long val2Ptr, int val2Len);
}

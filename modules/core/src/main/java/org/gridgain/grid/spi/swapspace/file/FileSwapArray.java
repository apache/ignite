/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace.file;

import org.jetbrains.annotations.*;

import java.util.concurrent.atomic.*;

/**
 * Growing array.
 */
class FileSwapArray<X> {
    /** First partition size must be power of two. */
    private static final int FIRST_ARRAY_SIZE = 4096;

    /** */
    private static final int LADDER_SIZE = Integer.numberOfLeadingZeros(FIRST_ARRAY_SIZE) + 1;

    /** */
    @SuppressWarnings("unchecked")
    private final AtomicReferenceArray<X>[] ladder = new AtomicReferenceArray[LADDER_SIZE];

    /** */
    private int idx = 1;

    /**
     *
     */
    FileSwapArray() {
        synchronized (ladder) {
            ladder[0] = new AtomicReferenceArray<>(FIRST_ARRAY_SIZE);
        }
    }

    /**
     * @return Size.
     */
    public int size() {
        return idx;
    }

    /**
     * Adds value to the end.
     *
     * @param x Value.
     * @return Index where it was added.
     */
    int add(X x) {
        int i = idx++;

        assert i >= 0 && i != Integer.MAX_VALUE : "Integer overflow";

        Slot<X> s = slot(i);

        assert s != null; // We should add always in one thread.

        s.set(x);

        int len = s.arr.length();

        if (s.idx + 1 == len) {
            synchronized (ladder) {
                ladder[s.arrIdx + 1] = new AtomicReferenceArray<>(s.arrIdx == 0 ? len : len << 1);
            }
        }

        return i;
    }

    /**
     * @param size New size.
     */
    void truncate(int size) {
        assert size > 0;

        idx = size;

        int arrIdx = arrayIndex(idx) + 1;

        if (arrIdx < ladder.length && ladder[arrIdx] != null) {
            synchronized (ladder) {
                do {
                    ladder[arrIdx++] = null;
                }
                while (arrIdx < ladder.length && ladder[arrIdx] != null);
            }
        }
    }

    /**
     * @param idx Absolute slot index.
     * @return Array index in {@link #ladder}.
     */
    static int arrayIndex(int idx) {
        if (idx < FIRST_ARRAY_SIZE)
            return 0;

        return LADDER_SIZE - Integer.numberOfLeadingZeros(idx);
    }

    /**
     * Slot for given absolute index.
     *
     * @param idx Absolute index.
     * @return Slot.
     */
    @Nullable Slot<X> slot(int idx) {
        assert idx > 0 : idx;

        int arrIdx = arrayIndex(idx);

        AtomicReferenceArray<X> arr = ladder[arrIdx];

        if (arr == null) {
            synchronized (ladder) { // Ensure visibility.
                arr = ladder[arrIdx];
            }

            if (arr == null)
                return null;
        }

        return new Slot<>(arrIdx, arr, arrIdx == 0 ? idx : idx - arr.length());
    }

    /**
     * Slot in array.
     */
    @SuppressWarnings("PublicInnerClass")
    static final class Slot<X> {
        /** */
        private final int arrIdx;

        /** */
        private final AtomicReferenceArray<X> arr;

        /** */
        private final int idx;

        /**
         * @param arrIdx Index of array.
         * @param arr Array.
         * @param idx Index within the array.
         */
        private Slot(int arrIdx, AtomicReferenceArray<X> arr, int idx) {
            this.arrIdx = arrIdx;
            this.arr = arr;
            this.idx = idx;
        }

        /**
         * @return Value.
         */
        public X get() {
            return arr.get(idx);
        }

        /**
         * @param exp Expected.
         * @param x New value.
         * @return {@code true} If succeeded.
         */
        public boolean cas(@Nullable X exp, @Nullable X x) {
            return exp == x || arr.compareAndSet(idx, exp, x);
        }

        /**
         * @param x value.
         */
        private void set(X x) {
            arr.lazySet(idx, x);
        }
    }
}

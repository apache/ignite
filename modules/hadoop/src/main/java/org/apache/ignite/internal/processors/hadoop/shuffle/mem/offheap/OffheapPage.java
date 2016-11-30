package org.apache.ignite.internal.processors.hadoop.shuffle.mem.offheap;

/**
 * Page.
 */
public class OffheapPage {
    /** Pointer. */
    private final long ptr;

    /** Size. */
    private final long size;

    /**
     * Constructor.
     *
     * @param ptr Pointer.
     * @param size Size.
     */
    OffheapPage(long ptr, long size) {
        this.ptr = ptr;
        this.size = size;
    }

    /**
     * @return Pointer.
     */
    long ptr() {
        return ptr;
    }

    /**
     * @return Page size.
     */
    long size() {
        return size;
    }
}

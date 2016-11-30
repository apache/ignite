package org.apache.ignite.internal.processors.hadoop.shuffle.mem.heap;

/**
 * Page.
 */
public class HeapPage {
    /** Pointer. */
    private byte [] buf;

    /** Size. */
    private final int order;

    /** Size. */
    private final long size;

    /**
     * Constructor.
     *
     * @param order Page order.
     * @param size Size.
     */
    HeapPage(int order, int size) {
        this.order = order;
        this.size = size;

        buf = new byte[size];
    }

    /**
     * @return Pointer.
     */
    long ptr() {
        return ((long)order) << 32;
    }

    /**
     * @return Page size.
     */
    long size() {
        return size;
    }

    /**
     * @return Page buffer.
     */
    byte[] buf() {
        return buf;
    }
}

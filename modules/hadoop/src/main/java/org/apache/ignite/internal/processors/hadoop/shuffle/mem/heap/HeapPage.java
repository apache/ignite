package org.apache.ignite.internal.processors.hadoop.shuffle.mem.heap;

import org.apache.ignite.internal.processors.hadoop.shuffle.mem.Page;

/**
 * Page.
 */
public class HeapPage implements Page {
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
    public HeapPage(int order, int size) {
        this.order = order;
        this.size = size;

        buf = new byte[size];
    }

    /**
     * @return Pointer.
     */
    public long ptr() {
        return ((long)order) << 32;
    }

    /**
     * @return Page size.
     */
    @Override public long size() {
        return size;
    }

    /**
     * @return Page buffer.
     */
    public byte[] buf() {
        return buf;
    }
}

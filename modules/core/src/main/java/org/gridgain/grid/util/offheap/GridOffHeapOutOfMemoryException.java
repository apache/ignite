/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap;

/**
 * Thrown when memory could not be allocated.
 */
public class GridOffHeapOutOfMemoryException extends RuntimeException {
    private static final long serialVersionUID = 0L;


    /**
     * Constructs out of memory exception.
     *
     * @param size Size that could not be allocated.
     */
    public GridOffHeapOutOfMemoryException(long size) {
        super("Failed to allocate memory: " + size);
    }

    /**
     * Constructs out of memory exception.
     *
     * @param total Total allocated memory.
     * @param size Size that could not be allocated.
     */
    public GridOffHeapOutOfMemoryException(long total, long size) {
        super("Failed to allocate memory [total=" + total + ", failed=" + size + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}

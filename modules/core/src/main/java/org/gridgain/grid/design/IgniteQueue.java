// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;
import org.gridgain.grid.design.configuration.*;

import java.util.concurrent.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface IgniteQueue<T> extends BlockingQueue<T> {
    /**
     *
     * @return Queue configuration.
     */
    public QueueConfiguration configuration();

    /**
     * Removes all of the elements from this queue. Method is used in massive queues with huge numbers of elements.
     *
     * @param batchSize Batch size.
     * @throws GridRuntimeException if operation failed.
     */
    public void clear(int batchSize) throws GridRuntimeException;

    /**
     * Gets maximum number of elements of the queue.
     *
     * @return Maximum number of elements. If queue is unbounded {@code Integer.MAX_SIZE} will return.
     * @throws GridException If operation failed.
     */
    public int capacity() throws GridException;

    /**
     * Returns {@code true} if this queue is bounded.
     *
     * @return {@code true} if this queue is bounded.
     * @throws GridException If operation failed.
     */
    public boolean bounded() throws GridException;

    /**
     * Returns {@code true} if this queue can be kept on the one node only.
     * Returns {@code false} if this queue can be kept on the many nodes.
     *
     * @return {@code true} if this queue is in {@code collocated} mode {@code false} otherwise.
     * @throws GridException If operation failed.
     */
    public boolean collocated() throws GridException;

    /**
     * Gets status of queue.
     *
     * @return {@code true} if queue was removed from cache {@code false} otherwise.
     */
    public boolean removed();
}

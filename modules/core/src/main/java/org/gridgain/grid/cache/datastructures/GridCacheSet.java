/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 * Set implementation based on on In-Memory Data Grid.
 * <h1 class="header">Overview</h1>
 * Cache set implements {@link Set} interface and provides all methods from collections.
 * Note that all {@link Collection} methods in the queue may throw {@link GridRuntimeException} in case of failure
 * or if set was removed.
 * <p>
 * During set iteration elements are loaded from remote cache nodes, if iteration is stopped before last element
 * is received it is necessary to close iterator to release all associated resources. {@link Iterator} interface
 * does not provide API for iteration stopping and cache set provides method {@link #iteratorEx} which
 * returns {@link GridCloseableIterator}, it supports iterator closing and also implements {@link AutoCloseable}.
 * For iteration over cache set method {@link #iteratorEx} should be always used instead of {@link #iterator}.
 * For safety reasons (to avoid resource leaks) method {@link GridCacheSet#iterator()}
 * throws {@link UnsupportedOperationException}.
 * <h1 class="header">Collocated vs Non-collocated</h1>
 * Set items can be placed on one node or distributed throughout grid nodes
 * (governed by {@code collocated} parameter). {@code Non-collocated} mode is provided only
 * for partitioned caches. If {@code collocated} parameter is {@code true}, then all queue items
 * will be collocated on one node, otherwise items will be distributed through all grid nodes.
 * @see GridCacheDataStructures#set(String, boolean, boolean)
 * @see GridCacheDataStructures#removeSet(String)
 */
public interface GridCacheSet<T> extends Set<T> {
    /**
     * Cache set iterator should be explicitly closed if iteration is stopped before last element is received,
     * {@link Iterator} interface does not support closing and for safety reason (to avoid resource leaks) this
     * method throws {@link UnsupportedOperationException}. For iteration over GridCacheSet method
     * {@link #iteratorEx} should be used.
     *
     * @deprecated Deprecated, use {@link #iteratorEx} instead.
     * @throws UnsupportedOperationException always.
     */
    @Deprecated
    @Override public Iterator<T> iterator() throws UnsupportedOperationException;

    /**
     * Gets closeable iterator over GridCacheSet. Note, if iteration is stopped before last element is received
     * itertor should be closed using {@link GridCloseableIterator#close} to avoid resource leaks.
     *
     * @return Closeable iterator.
     */
    public GridCloseableIterator<T> iteratorEx();

    /**
     * Gets set name.
     *
     * @return Set name.
     */
    public String name();

    /**
     * Returns {@code true} if this set can be kept on the one node only.
     * Returns {@code false} if this set can be kept on the many nodes.
     *
     * @return {@code True} if this set is in {@code collocated} mode {@code false} otherwise.
     * @throws GridException If operation failed.
     */
    public boolean collocated() throws GridException;

    /**
     * Gets status of set.
     *
     * @return {@code True} if set was removed from cache {@code false} otherwise.
     */
    public boolean removed();
}

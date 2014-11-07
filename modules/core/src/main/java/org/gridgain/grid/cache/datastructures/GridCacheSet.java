/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.gridgain.grid.*;

import java.util.*;

/**
 * Set implementation based on on In-Memory Data Grid.
 * <h1 class="header">Overview</h1>
 * Cache set implements {@link Set} interface and provides all methods from collections.
 * Note that all {@link Collection} methods in the set may throw {@link GridRuntimeException} in case of failure
 * or if set was removed.
 * <h1 class="header">Collocated vs Non-collocated</h1>
 * Set items can be placed on one node or distributed throughout grid nodes
 * (governed by {@code collocated} parameter). {@code Non-collocated} mode is provided only
 * for partitioned caches. If {@code collocated} parameter is {@code true}, then all set items
 * will be collocated on one node, otherwise items will be distributed through all grid nodes.
 * @see GridCacheDataStructures#set(String, boolean, boolean)
 * @see GridCacheDataStructures#removeSet(String)
 */
public interface GridCacheSet<T> extends Set<T> {
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

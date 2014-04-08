/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 */
public interface GridCacheSet<T> extends Set<T> {
    /**
     * @deprecated Deprecated.
     * @throws UnsupportedOperationException always.
     */
    @Deprecated
    @Override public Iterator<T> iterator() throws UnsupportedOperationException;

    /**
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
}

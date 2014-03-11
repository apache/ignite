/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import java.util.*;

/**
 * Defines "rich" iterable interface that is also acts as lambda function and iterator.
 * @see GridIterator
 */
public interface GridIterable<T> extends GridIterator<T> {
    /**
     * Returns {@link GridIterator} which extends regular {@link Iterator} interface
     * and adds methods that account for possible failures in cases when iterating
     * over data that has been partially received over network.
     *
     * @return Instance of new {@link GridIterator}.
     */
    @Override public GridIterator<T> iterator();
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.ignite.*;
import org.gridgain.grid.util.lang.*;

/**
 * Empty closeable iterator.
 */
public class GridEmptyCloseableIterator<T> extends GridEmptyIterator<T> implements GridCloseableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Closed flag. */
    private boolean closed;

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        closed = true;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }
}

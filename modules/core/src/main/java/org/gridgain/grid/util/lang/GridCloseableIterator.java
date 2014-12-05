/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.spi.*;
import org.gridgain.grid.*;

/**
 * Defines "rich" closeable iterator interface that is also acts like lambda function and iterable.
 * <p>
 * Any instance of this interface should be closed when it's no longer needed.
 * <p>
 * Here is the common use of closable iterator.
 * <pre>
 * GridCloseableIterator<T> iter = getIterator();
 *
 * try {
 *     while(iter.hasNext()) {
 *         T next = iter.next();
 *         ...
 *         ...
 *     }
 * }
 * finally {
 *     iter.close();
 * }
 * </pre>
 */
public interface GridCloseableIterator<T> extends GridIterator<T>, IgniteSpiCloseableIterator<T>, AutoCloseable {
    /**
     * Closes the iterator and frees all the resources held by the iterator.
     * Iterator can not be used any more after calling this method.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws GridException In case of error.
     */
    @Override public void close() throws GridException;

    /**
     * Checks if iterator has been closed.
     *
     * @return {@code True} if iterator has been closed.
     */
    public boolean isClosed();
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi;

import org.apache.ignite.*;

import java.io.*;
import java.util.*;

/**
 *  Closeable iterator.
 */
public interface IgniteSpiCloseableIterator<T> extends Iterator<T>, AutoCloseable, Serializable {
    /**
     * Closes the iterator and frees all the resources held by the iterator.
     * Iterator can not be used any more after calling this method.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws IgniteCheckedException In case of error.
     */
    @Override public void close() throws IgniteCheckedException;
}

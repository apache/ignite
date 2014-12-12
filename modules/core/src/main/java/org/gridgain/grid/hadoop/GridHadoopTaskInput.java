/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.apache.ignite.*;

import java.util.*;

/**
 * Task input.
 */
public interface GridHadoopTaskInput extends AutoCloseable {
    /**
     * Moves cursor to the next element.
     *
     * @return {@code false} If input is exceeded.
     */
    boolean next();

    /**
     * Gets current key.
     *
     * @return Key.
     */
    Object key();

    /**
     * Gets values for current key.
     *
     * @return Values.
     */
    Iterator<?> values();

    /**
     * Closes input.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Override public void close() throws IgniteCheckedException;
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.lang;

import java.io.*;

/**
 * Closure that does not take any parameters and returns a value.
 *
 * @param <T> Type of return value from this closure.
 */
public interface IgniteOutClosure<T> extends Serializable {
    /**
     * Closure body.
     *
     * @return Return value.
     */
    public T apply();
}

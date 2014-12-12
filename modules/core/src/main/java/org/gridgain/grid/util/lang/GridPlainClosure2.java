/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.*;

/**
 * Closure taking 2 arguments.
 */
public interface GridPlainClosure2<T1, T2, R> {
    /**
     * @param arg1 Closure argument.
     * @param arg2 Closure argument.
     * @return Closure execution result.
     * @throws IgniteCheckedException If error occurred.
     */
    public R apply(T1 arg1, T2 arg2) throws IgniteCheckedException;
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.lang.*;

/**
 * Transform closure additionally computing return value, used by cache operation
 * {@link GridCacheAdapter#transformCompute(Object, GridCacheTransformComputeClosure)}.
 */
public interface GridCacheTransformComputeClosure<V, R> extends GridClosure<V, V> {
    /**
     * @param val Old value.
     * @return Value to be returned as result of {@link GridCacheAdapter#transformCompute}.
     */
    public R compute(V val);
}

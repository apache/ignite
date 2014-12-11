/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.jta;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

/**
 * Provides possibility to integrate cache transactions with JTA.
 */
public abstract class GridCacheJtaManagerAdapter<K, V> extends GridCacheManagerAdapter<K, V> {
    /**
     * Creates transaction manager finder.
     *
     * @param ccfg Cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void createTmLookup(GridCacheConfiguration ccfg) throws IgniteCheckedException;

    /**
     * Checks if cache is working in JTA transaction and enlist cache as XAResource if necessary.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public abstract void checkJta() throws IgniteCheckedException;

    /**
     * Gets transaction manager finder. Returns Object to avoid dependency on JTA library.
     *
     * @return Transaction manager finder.
     */
    @Nullable public abstract Object tmLookup();
}

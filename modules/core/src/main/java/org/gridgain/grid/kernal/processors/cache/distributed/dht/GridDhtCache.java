/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * DHT cache.
 */
public class GridDhtCache<K, V> extends GridDhtTransactionalCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near cache. */
    @GridToStringExclude
    private GridNearTransactionalCache<K, V> near;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtCache() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridDhtCache(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * @param ctx Cache context.
     * @param map Cache map.
     */
    public GridDhtCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public boolean isDht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        String name = super.name();

        return name == null ? "defaultDhtCache" : name + "Dht";
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        resetMetrics();

        super.start();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        GridCacheMetricsAdapter m = new GridCacheMetricsAdapter();

        m.delegate(ctx.dht().near().metrics0());

        metrics = m;

        ctx.dr().resetMetrics();
    }

    /**
     * @return Near cache.
     */
    @Override public GridNearTransactionalCache<K, V> near() {
        return near;
    }

    /**
     * @param near Near cache.
     */
    public void near(GridNearTransactionalCache<K, V> near) {
        this.near = near;
    }
}

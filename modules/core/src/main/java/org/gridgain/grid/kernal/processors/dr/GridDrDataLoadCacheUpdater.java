/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Data center replication cache updater for data loader.
 */
public class GridDrDataLoadCacheUpdater<K, V> implements GridDataLoadCacheUpdater<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void update(GridCache<K, V> cache0, Collection<Map.Entry<K, V>> col)
        throws GridException {
        String cacheName = cache0.name();

        GridKernalContext ctx = ((GridKernal)cache0.gridProjection().grid()).context();
        GridLogger log = ctx.log(GridDrDataLoadCacheUpdater.class);
        GridCacheAdapter<K, V> cache = ctx.cache().internalCache(cacheName);

        assert !F.isEmpty(col);

        if (log.isDebugEnabled())
            log.debug("Running DR put job [nodeId=" + ctx.localNodeId() + ", cacheName=" + cacheName + ']');

        GridFuture<?> f = cache.context().preloader().startFuture();

        if (!f.isDone())
            f.get();

        for (Map.Entry<K, V> entry0 : col) {
            GridVersionedEntry<K, V> entry = (GridVersionedEntry<K, V>)entry0;

            entry.unmarshal(ctx.config().getMarshaller());

            K key = entry.key();

            GridCacheDrInfo<V> val = entry.value() != null ? entry.expireTime() != 0 ?
                new GridCacheDrExpirationInfo<>(entry.value(), entry.version(), entry.ttl(), entry.expireTime()) :
                new GridCacheDrInfo<>(entry.value(), entry.version()) : null;

            if (val == null)
                cache.removeAllDr(Collections.singletonMap(key, entry.version()));
            else
                cache.putAllDr(Collections.singletonMap(key, val));
        }

        if (log.isDebugEnabled())
            log.debug("DR put job finished [nodeId=" + ctx.localNodeId() + ", cacheName=" + cacheName + ']');
    }
}

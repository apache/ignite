// @java.file.header

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

import java.io.*;
import java.util.*;

/**
 * Data center replication cache updater for data loader.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrDataLoadCacheUpdater<K, V> implements GridDataLoadCacheUpdater<K, V>, Externalizable {
    /**  */
    private int keysCnt = 100;

    /**
     * For {@link Externalizable}.
     */
    public GridDrDataLoadCacheUpdater() {
        // No-op.
    }

    /**
     * @param keysCnt Keys per transaction.
     */
    public GridDrDataLoadCacheUpdater(int keysCnt) {
        assert keysCnt > 0 : keysCnt;

        this.keysCnt = keysCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(keysCnt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        keysCnt = in.readInt();
    }

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

        Map<K, GridCacheDrInfo<V>> putMap = null;
        Map<K, GridCacheVersion> rmvMap = null;

        for (Map.Entry<K, V> entry0 : col) {
            GridDrRawEntry<K, V> entry = (GridDrRawEntry<K, V>)entry0;

            entry.unmarshal(ctx.config().getMarshaller());

            K key = entry.key();

            GridCacheDrInfo<V> val = entry.value() != null ? entry.expireTime() != 0 ?
                new GridCacheDrExpirationInfo<>(entry.value(), entry.version(), entry.ttl(), entry.expireTime()) :
                new GridCacheDrInfo<>(entry.value(), entry.version()) : null;

            if (key instanceof Comparable) {
                if (val == null) {
                    if (rmvMap == null)
                        rmvMap = new TreeMap<>();

                    GridCacheVersion oldVer = rmvMap.put(key, entry.version());

                    if (rmvMap.size() >= keysCnt || oldVer != null) {
                        cache.removeAllDr(rmvMap);

                        if (oldVer == null)
                            rmvMap = null;
                        else {
                            rmvMap = new TreeMap<>();

                            rmvMap.put(key, oldVer);
                        }
                    }
                }
                else {
                    if (putMap == null)
                        putMap = new TreeMap<>();

                    GridCacheDrInfo<V> oldVal = putMap.put(key, val);

                    if (putMap.size() >= keysCnt || oldVal != null) {
                        cache.putAllDr(putMap);

                        if (oldVal == null)
                            putMap = null;
                        else {
                            putMap = new TreeMap<>();

                            putMap.put(key, oldVal);
                        }
                    }
                }
            }
            else {
                if (val == null)
                    cache.removeAllDr(Collections.singletonMap(key, entry.version()));
                else
                    cache.putAllDr(Collections.singletonMap(key, val));
            }
        }

        if (putMap != null)
            cache.putAllDr(putMap);

        if (rmvMap != null)
            cache.removeAllDr(rmvMap);

        if (log.isDebugEnabled())
            log.debug("DR put job finished [nodeId=" + ctx.localNodeId() + ", cacheName=" + cacheName + ']');
    }
}

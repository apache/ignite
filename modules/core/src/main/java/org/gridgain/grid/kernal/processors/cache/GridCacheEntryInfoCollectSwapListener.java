/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.util.*;

/**
 *
 */
public class GridCacheEntryInfoCollectSwapListener<K, V> implements GridCacheSwapListener<K, V> {
    /** */
    private final Map<K, GridCacheEntryInfo<K, V>> swappedEntries = new ConcurrentHashMap8<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final GridCacheContext<K, V> ctx;

    /**
     * @param log Logger.
     * @param ctx Context.
     */
    public GridCacheEntryInfoCollectSwapListener(IgniteLogger log, GridCacheContext<K, V> ctx) {
        this.log = log;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void onEntryUnswapped(int part, K key, byte[] keyBytes, GridCacheSwapEntry<V> swapEntry) {
        try {
            if (log.isDebugEnabled())
                log.debug("Received unswapped event for key: " + key);

            assert key != null;
            assert swapEntry != null;

            GridCacheEntryInfo<K, V> info = new GridCacheEntryInfo<>();

            info.keyBytes(keyBytes);
            info.ttl(swapEntry.ttl());
            info.expireTime(swapEntry.expireTime());
            info.version(swapEntry.version());

            if (!swapEntry.valueIsByteArray()) {
                boolean convertPortable = ctx.portableEnabled() && ctx.offheapTiered();

                if (convertPortable)
                    info.valueBytes(ctx.convertPortableBytes(swapEntry.valueBytes()));
                else
                    info.valueBytes(swapEntry.valueBytes());
            }
            else
                swapEntry.value(swapEntry.value());

            swappedEntries.put(key, info);
        }
        catch (GridException e) {
            U.error(log, "Failed to process unswapped entry", e);
        }
    }

    /**
     * @return Entries, received by listener.
     */
    public Collection<GridCacheEntryInfo<K, V>> entries() {
        return swappedEntries.values();
    }
}

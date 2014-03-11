/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Runnable for {@link GridCacheAdapter#clearAll()} routine for near cache.
 */
public class GridNearCacheClearAllRunnable<K, V> extends GridCacheClearAllRunnable<K, V> {
    /** Runnable for DHT cache. */
    private final GridCacheClearAllRunnable<K, V> dhtJob;

    /**
     * Constructor.
     *
     * @param cache Cache to be cleared.
     * @param obsoleteVer Obsolete version.
     * @param dhtJob Linked DHT job.
     */
    public GridNearCacheClearAllRunnable(GridCacheAdapter<K, V> cache, GridCacheVersion obsoleteVer,
        GridCacheClearAllRunnable<K, V> dhtJob) {
        super(cache, obsoleteVer, dhtJob.id(), dhtJob.totalCount());

        assert dhtJob != null;

        this.dhtJob = dhtJob;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            // Delegate to DHT cache first.
            if (dhtJob != null)
                dhtJob.run();
        }
        finally {
            super.run();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearCacheClearAllRunnable.class, this);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task for swapping backup cache entries.
 */
@GridInternal
public class VisorCacheSwapBackupsTask extends VisorOneNodeTask<Set<String>, Map<String,
    IgniteBiTuple<Integer, Integer>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesSwapBackupsJob job(Set<String> names) {
        return new VisorCachesSwapBackupsJob(names, debug);
    }

    /**
     * Job that swap backups.
     */
    private static class VisorCachesSwapBackupsJob extends VisorJob<Set<String>, Map<String,
        IgniteBiTuple<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param names Job argument.
         * @param debug Debug flag.
         */
        private VisorCachesSwapBackupsJob(Set<String> names, boolean debug) {
            super(names, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, IgniteBiTuple<Integer, Integer>> run(Set<String> names) throws GridException {
            Map<String, IgniteBiTuple<Integer, Integer>> total = new HashMap<>();

            for (GridCache c: g.cachesx()) {
                String cacheName = c.name();

                if (names.contains(cacheName)) {
                    Set<GridCacheEntry> entries = c.entrySet();

                    int before = entries.size(), after = before;

                    for (GridCacheEntry entry: entries) {
                        if (entry.backup() && entry.evict())
                            after--;
                    }

                    total.put(cacheName, new IgniteBiTuple<>(before, after));
                }
            }

            return total;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesSwapBackupsJob.class, this);
        }
    }
}

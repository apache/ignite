/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task that compacts caches.
 */
@GridInternal
public class VisorCacheCompactTask extends VisorOneNodeTask<Set<String>, Map<String, IgniteBiTuple<Integer, Integer>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesCompactJob job(Set<String> names) {
        return new VisorCachesCompactJob(names, debug);
    }

    /** Job that compact caches on node. */
    private static class VisorCachesCompactJob extends VisorJob<Set<String>, Map<String, IgniteBiTuple<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param names Cache names to compact.
         * @param debug Debug flag.
         */
        private VisorCachesCompactJob(Set<String> names, boolean debug) {
            super(names, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, IgniteBiTuple<Integer, Integer>> run(Set<String> names) throws IgniteCheckedException {
            final Map<String, IgniteBiTuple<Integer, Integer>> res = new HashMap<>();

            for(GridCache cache : g.cachesx()) {
                String cacheName = cache.name();

                if (names.contains(cacheName)) {
                    final Set keys = cache.keySet();

                    int before = keys.size(), after = before;

                    for (Object key : keys) {
                        if (cache.compact(key))
                            after--;
                    }

                    res.put(cacheName, new IgniteBiTuple<>(before, after));
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesCompactJob.class, this);
        }
    }
}

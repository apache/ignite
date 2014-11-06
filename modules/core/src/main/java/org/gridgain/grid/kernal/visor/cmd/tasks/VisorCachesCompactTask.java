/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task that compacts caches.
 */
@GridInternal
public class VisorCachesCompactTask extends VisorOneNodeTask<Set<String>, Map<String, GridBiTuple<Integer, Integer>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesCompactJob job(Set<String> names) {
        return new VisorCachesCompactJob(names);
    }

    /** Job that compact caches on node. */
    private static class VisorCachesCompactJob extends VisorJob<Set<String>, Map<String, GridBiTuple<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        private VisorCachesCompactJob(Set<String> names) {
            super(names);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, GridBiTuple<Integer, Integer>> run(Set<String> names) throws GridException {
            final Map<String, GridBiTuple<Integer, Integer>> res = new HashMap<>();

            for(GridCache cache : g.cachesx()) {
                String cacheName = cache.name();

                if (names.contains(cacheName)) {
                    final Set keys = cache.keySet();

                    int before = keys.size(), after = before;

                    for (Object key : keys) {
                        if (cache.compact(key))
                            after--;
                    }

                    res.put(cacheName, new GridBiTuple<>(before, after));
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

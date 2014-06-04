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
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task for swapping backup cache entries.
 */
@GridInternal
public class VisorSwapBackupsCachesTask extends VisorOneNodeTask<Set<String>, Map<String, T2<Integer, Integer>> > {
    /**
     * Job that swap backups.
     */
    private static class VisorSwapBackupsCachesJob extends VisorJob<Set<String>, Map<String, T2<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param names Job argument.
         */
        private VisorSwapBackupsCachesJob(Set<String> names) {
            super(names);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, T2<Integer, Integer>> run(Set<String> names) throws GridException {
            Map<String, T2<Integer, Integer>> total = new HashMap<>();

            for (GridCache c: g.cachesx()) {
                String cacheName = c.name();

                if (names.contains(cacheName)) {
                    Set<GridCacheEntry> entries = c.entrySet();

                    int before = entries.size(), after = before;

                    for (GridCacheEntry entry: entries) {
                        if (entry.backup() && entry.evict())
                            after--;
                    }

                    total.put(cacheName, new T2<>(before, after));
                }
            }

            return total;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorSwapBackupsCachesJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorSwapBackupsCachesJob job(Set<String> names) {
        return new VisorSwapBackupsCachesJob(names);
    }
}

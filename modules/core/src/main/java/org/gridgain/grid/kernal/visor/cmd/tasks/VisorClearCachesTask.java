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

import java.util.*;

/**
 * Task that clears specified caches on specified node.
 */
@GridInternal
public class VisorClearCachesTask extends VisorOneNodeTask<Set<String>, Map<String, T2<Integer, Integer>>> {
    /**
     * Job that clear specified caches.
     */
    private static class VisorClearCachesJob extends VisorJob<Set<String>, Map<String, T2<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Job argument.
         */
        private VisorClearCachesJob(Set<String> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, T2<Integer, Integer>> run(Set<String> arg) throws GridException {
            Map<String, T2<Integer, Integer>> res = new HashMap<>();

            for(GridCache cache : g.cachesx()) {
                String cacheName = cache.name();

                if (arg.contains(cacheName)) {
                    Set keys = cache.keySet();

                    int before = keys.size(), after = before;

                    for (Object key : keys) {
                        if (cache.clear(key))
                            after--;
                    }

                    res.put(cacheName, new T2<>(before, after));
                }
            }

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorClearCachesJob job(Set<String> arg) {
        return new VisorClearCachesJob(arg);
    }
}

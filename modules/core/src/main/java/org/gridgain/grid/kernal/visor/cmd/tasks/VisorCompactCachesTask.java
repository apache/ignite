/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.*;

import java.util.*;

/**
 * Compacts caches.
 */
@GridInternal
public class VisorCompactCachesTask extends VisorOneNodeTask<VisorOneNodeCachesArg, VisorCachesTaskResult> {
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCompactCachesJob
        extends VisorOneNodeJob<VisorOneNodeCachesArg, VisorCachesTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorCompactCachesJob(VisorOneNodeCachesArg arg) {
            super(arg);
        }

        @Override
        protected VisorCachesTaskResult run(VisorOneNodeCachesArg arg) throws GridException {
            final VisorCachesTaskResult res = new VisorCachesTaskResult();

            for(GridCache cache : g.cachesx(null)) {
                String cacheName = cache.name();

                if (arg.cacheNames().contains(cacheName)) {
                    final Set keys = cache.keySet();

                    long before = keys.size(), after = before;

                    for (Object key : keys) {
                        if (cache.compact(key))
                            after--;
                    }

                    res.put(cacheName, new VisorBeforeAfterResult(before, after));
                }
            }

            return res;
        }
    }

    @Override
    protected VisorJob<VisorOneNodeCachesArg, VisorCachesTaskResult> job(VisorOneNodeCachesArg arg) {
        return new VisorCompactCachesJob(arg);
    }
}

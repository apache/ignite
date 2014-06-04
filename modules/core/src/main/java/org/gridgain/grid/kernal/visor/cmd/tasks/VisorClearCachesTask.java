/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.*;

import java.util.*;

/**
 * Task that clears specified caches on specified node.
 */
@GridInternal
public class VisorClearCachesTask extends VisorOneNodeTask<VisorOneNodeNamesArg, VisorNamedBeforeAfterTaskResult> {
    /**
     * Job that clear specified caches.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorClearCachesJob extends VisorOneNodeJob<VisorOneNodeNamesArg, VisorNamedBeforeAfterTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Job argument.
         */
        public VisorClearCachesJob(VisorOneNodeNamesArg arg) {
            super(arg);
        }

        @Override protected VisorNamedBeforeAfterTaskResult run(VisorOneNodeNamesArg arg) throws GridException {
            VisorNamedBeforeAfterTaskResult res = new VisorNamedBeforeAfterTaskResult();

            for(GridCache cache : g.cachesx()) {
                String cacheName = cache.name();

                if (arg.names().contains(cacheName)) {
                    Set keys = cache.keySet();

                    long before = keys.size(), after = before;

                    for (Object key : keys) {
                        if (cache.clear(key))
                            after--;
                    }

                    res.put(cacheName, new VisorBeforeAfterResult(before, after));
                }
            }

            return res;
        }
    }

    @Override protected VisorClearCachesJob job(VisorOneNodeNamesArg arg) {
        return new VisorClearCachesJob(arg);
    }
}

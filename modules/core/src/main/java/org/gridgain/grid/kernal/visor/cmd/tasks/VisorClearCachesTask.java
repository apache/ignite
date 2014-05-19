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
import org.gridgain.grid.kernal.visor.cmd.dto.*;

import static org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils.escapeName;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Task that clears specified caches on specified node.
 */
@GridInternal
public class VisorClearCachesTask extends
    VisorOneNodeTask<VisorOneNodeCachesArg, VisorClearCachesTask.VisorClearCachesTaskResult> {
    /**
     * {@link VisorClearCachesTask} task result item with count of cleared keys and cache size after clear.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorClearCachesTaskResultItem implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cleared keys count. */
        private final int cleared;

        /** Cache size after clear. */
        private final int after;

        /**
         * Create result item.
         *
         * @param cleared Cleared keys count.
         * @param after Cache size after clear.
         */
        public VisorClearCachesTaskResultItem(int cleared, int after) {
            this.cleared = cleared;
            this.after = after;
        }

        /**
         * @return Cleared keys count.
         */
        public int cleared() {
            return cleared;
        }

        /**
         * @return Cache size after clear.
         */
        public int after() {
            return after;
        }
    }

    /**
     * {@link VisorClearCachesTask} task result type.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorClearCachesTaskResult extends HashMap<String, VisorClearCachesTaskResultItem>{
        /** */
        private static final long serialVersionUID = 0L;
    }

    /**
     * Job for {@link VisorClearCachesTask} task that clear specified caches.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorClearCachesJob extends VisorOneNodeJob<VisorOneNodeCachesArg, VisorClearCachesTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Job argument.
         */
        public VisorClearCachesJob(VisorOneNodeCachesArg arg) {
            super(arg);
        }

        @Override
        protected VisorClearCachesTaskResult run(VisorOneNodeCachesArg arg) throws GridException {
            VisorClearCachesTaskResult res = new VisorClearCachesTaskResult();

            for(GridCache cache : g.cachesx(null)) {
                String escapedName = escapeName(cache.name());

                if (arg.cacheNames().contains(escapedName)) {
                    int cleared = 0;

                    for (Object key : cache.keySet()) {
                        if (cache.clear(key))
                            cleared++;
                    }

                    res.put(escapedName, new VisorClearCachesTaskResultItem(cleared, cache.size()));
                }
            }

            return res;
        }
    }

    @Override
    protected VisorJob<VisorOneNodeCachesArg, VisorClearCachesTaskResult> job(VisorOneNodeCachesArg arg) {
        return new VisorClearCachesJob(arg);
    }
}

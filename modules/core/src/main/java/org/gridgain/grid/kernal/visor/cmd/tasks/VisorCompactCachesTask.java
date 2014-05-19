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
import org.gridgain.grid.kernal.visor.cmd.dto.*;

import java.io.Serializable;
import java.util.*;

import static org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils.*;

/**
 * Compacts caches.
 */
@GridInternal
public class VisorCompactCachesTask extends VisorOneNodeTask<VisorOneNodeCachesArg,
    VisorCompactCachesTask.VisorCompactCachesTaskResult> {
    /**
     * {@link VisorCompactCachesTask} task result item with count of compacted keys and cache size after compact.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCompactCachesTaskResultItem implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Compacted keys count. */
        private final int compacted;

        /** Cache size after compact. */
        private final int after;

        /**
         * Create result item.
         *
         * @param compacted Cleared keys count.
         * @param after Cache size after clear.
         */
        public VisorCompactCachesTaskResultItem(int compacted, int after) {
            this.compacted = compacted;
            this.after = after;
        }

        /**
         * @return compacted keys count.
         */
        public int compacted() {
            return compacted;
        }

        /**
         * @return Cache size after compact.
         */
        public int after() {
            return after;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorCompactCachesTaskResult extends HashMap<String, VisorCompactCachesTaskResultItem> {
        /** */
        private static final long serialVersionUID = 0L;
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorCompactCachesJob
        extends VisorOneNodeJob<VisorOneNodeCachesArg, VisorCompactCachesTaskResult> {
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
        protected VisorCompactCachesTaskResult run(VisorOneNodeCachesArg arg) throws GridException {
            VisorCompactCachesTaskResult res = new VisorCompactCachesTaskResult();

            for(GridCache cache : g.cachesx(null)) {
                String escapedName = escapeName(cache.name());

                if (arg.cacheNames().contains(escapedName)) {
                    int compacted = 0;

                    for (Object key : cache.keySet()) {
                        if (cache.compact(key))
                            compacted++;
                    }

                    res.put(escapedName, new VisorCompactCachesTaskResultItem(compacted, cache.size()));
                }
            }

            return res;
        }
    }

    @Override
    protected VisorJob<VisorOneNodeCachesArg, VisorCompactCachesTaskResult> job(VisorOneNodeCachesArg arg) {
        return new VisorCompactCachesJob(arg);
    }
}

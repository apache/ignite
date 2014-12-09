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
 * Pre-loads caches. Made callable just to conform common pattern.
 */
@GridInternal
public class VisorCachePreloadTask extends VisorOneNodeTask<Set<String>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesPreloadJob job(Set<String> arg) {
        return new VisorCachesPreloadJob(arg, debug);
    }

    /**
     * Job that preload caches.
     */
    private static class VisorCachesPreloadJob extends VisorJob<Set<String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Caches names.
         * @param debug Debug flag.
         */
        private VisorCachesPreloadJob(Set<String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Set<String> cacheNames) throws GridException {
            Collection<IgniteFuture<?>> futs = new ArrayList<>();

            for(GridCache c : g.cachesx()) {
                if (cacheNames.contains(c.name()))
                    futs.add(c.forceRepartition());
            }

            for (IgniteFuture f: futs)
                f.get();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesPreloadJob.class, this);
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.File;

/**
 * Marshaller cache listener.
 */
public class MarshallerCacheListener {
    /** */
    private ContinuousQueryListener lsnr;

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx, String cacheName, File workDir)
        throws IgniteCheckedException {
        assert ctx != null;
        assert cacheName != null;
        assert workDir != null;

        if (ctx.clientNode()) {
            lsnr = new ContinuousQueryListener(ctx.log(MarshallerCacheListener.class), workDir);

            ctx.continuous().registerStaticRoutine(
                cacheName,
                lsnr,
                null,
                null);
        }
    }

    /**
     * @param ctx Kernal context.
     * @param cache Cache.
     *@param log Log.
     */
    public void onMarshallerCacheStarted(GridKernalContext ctx, IgniteInternalCache<Object,String> cache,
        final IgniteLogger log, File workDir) throws IgniteCheckedException {
        assert ctx != null;
        assert cache != null;
        assert log != null;
        assert workDir != null;

        final GridCacheContext<Object, String> cacheCtx = cache.context();

        if (cacheCtx.affinityNode()) {
            cacheCtx.continuousQueries().executeInternalQuery(
                new ContinuousQueryListener(log, workDir),
                null,
                true,
                true,
                false
            );
        }
        else {
            if (lsnr != null) {
                ctx.closure().runLocalSafe(new Runnable() {
                    @SuppressWarnings("unchecked")
                    @Override public void run() {
                        try {
                            Iterable entries = cacheCtx.continuousQueries().existingEntries(false, null);

                            lsnr.onUpdated(entries);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to load marshaller cache entries: " + e, e);
                        }
                    }
                });
            }
        }
    }

    /**
     */
    public static class ContinuousQueryListener implements CacheEntryUpdatedListener<Object, String> {
        /** */
        private final IgniteLogger log;

        /** */
        private final File workDir;

        /**
         * @param log Logger.
         * @param workDir Work directory.
         */
        public ContinuousQueryListener(IgniteLogger log, File workDir) {
            this.log = log;
            this.workDir = workDir;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ? extends String>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<?, ? extends String> evt : evts) {
                assert evt.getOldValue() == null || F.eq(evt.getOldValue(), evt.getValue()):
                    "Received cache entry update for system marshaller cache: " + evt;

                if (evt.getOldValue() == null)
                    MarshallerWorkDirectory.writeTypeNameToFile(evt.getKey(), evt.getValue(), log, workDir);
            }
        }
    }
}

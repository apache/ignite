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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Job to put entries to cache on affinity node.
 */
class DataStreamerUpdateJob implements GridPlainCallable<Object> {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** Cache name. */
    private final String cacheName;

    /** Entries to put. */
    private final Collection<DataStreamerEntry> col;

    /** {@code True} to ignore deployment ownership. */
    private final boolean ignoreDepOwnership;

    /** */
    private final boolean skipStore;

    /** */
    private final IgniteDataStreamer.Updater updater;

    /**
     * @param ctx Context.
     * @param log Log.
     * @param cacheName Cache name.
     * @param col Entries to put.
     * @param ignoreDepOwnership {@code True} to ignore deployment ownership.
     * @param skipStore Skip store flag.
     * @param updater Updater.
     */
    DataStreamerUpdateJob(
        GridKernalContext ctx,
        IgniteLogger log,
        @Nullable String cacheName,
        Collection<DataStreamerEntry> col,
        boolean ignoreDepOwnership,
        boolean skipStore,
        IgniteDataStreamer.Updater<?, ?> updater) {
        this.ctx = ctx;
        this.log = log;

        assert col != null && !col.isEmpty();
        assert updater != null;

        this.cacheName = cacheName;
        this.col = col;
        this.ignoreDepOwnership = ignoreDepOwnership;
        this.skipStore = skipStore;
        this.updater = updater;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object call() throws Exception {
        if (log.isDebugEnabled())
            log.debug("Running put job [nodeId=" + ctx.localNodeId() + ", size=" + col.size() + ']');

//        TODO IGNITE-77: restore adapter usage.
//        GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);
//
//        IgniteFuture<?> f = cache.context().preloader().startFuture();
//
//        if (!f.isDone())
//            f.get();
//
//        if (ignoreDepOwnership)
//            cache.context().deploy().ignoreOwnership(true);

        IgniteCacheProxy cache = ctx.cache().jcache(cacheName);

        if (skipStore)
            cache = (IgniteCacheProxy<?, ?>)cache.withSkipStore();

        if (ignoreDepOwnership)
            cache.context().deploy().ignoreOwnership(true);

        try {
            final GridCacheContext cctx = cache.context();

            for (DataStreamerEntry e : col) {
                e.getKey().finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

                CacheObject val = e.getValue();

                if (val != null)
                    val.finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());
            }

            if (unwrapEntries()) {
                Collection<Map.Entry> col0 = F.viewReadOnly(col, new C1<DataStreamerEntry, Map.Entry>() {
                    @Override public Map.Entry apply(DataStreamerEntry e) {
                        return e.toEntry(cctx);
                    }
                });

                updater.update(cache, col0);
            }
            else
                updater.update(cache, col);

            return null;
        }
        finally {
            if (ignoreDepOwnership)
                cache.context().deploy().ignoreOwnership(false);

            if (log.isDebugEnabled())
                log.debug("Update job finished on node: " + ctx.localNodeId());
        }
    }

    /**
     * @return {@code True} if need to unwrap internal entries.
     */
    private boolean unwrapEntries() {
        return !(updater instanceof DataStreamerCacheUpdaters.InternalUpdater);
    }
}

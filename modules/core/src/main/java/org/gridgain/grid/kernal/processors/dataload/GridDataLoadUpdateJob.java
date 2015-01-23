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

package org.gridgain.grid.kernal.processors.dataload;

import org.apache.ignite.*;
import org.apache.ignite.dataload.*;
import org.apache.ignite.internal.processors.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Job to put entries to cache on affinity node.
 */
class GridDataLoadUpdateJob<K, V> implements GridPlainCallable<Object> {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** Cache name. */
    private final String cacheName;

    /** Entries to put. */
    private final Collection<Map.Entry<K, V>> col;

    /** {@code True} to ignore deployment ownership. */
    private final boolean ignoreDepOwnership;

    /** */
    private final boolean skipStore;

    /** */
    private final IgniteDataLoadCacheUpdater<K, V> updater;

    /**
     * @param ctx Context.
     * @param log Log.
     * @param cacheName Cache name.
     * @param col Entries to put.
     * @param ignoreDepOwnership {@code True} to ignore deployment ownership.
     * @param updater Updater.
     */
    GridDataLoadUpdateJob(
        GridKernalContext ctx,
        IgniteLogger log,
        @Nullable String cacheName,
        Collection<Map.Entry<K, V>> col,
        boolean ignoreDepOwnership,
        boolean skipStore,
        IgniteDataLoadCacheUpdater<K, V> updater) {
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

        IgniteCacheProxy<K, V> cache = ctx.cache().jcache(cacheName);

        if (skipStore)
            cache = (IgniteCacheProxy<K, V>)cache.withSkipStore();

        if (ignoreDepOwnership)
            cache.context().deploy().ignoreOwnership(true);

        try {
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
}

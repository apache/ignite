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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;

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
    private final StreamReceiver rcvr;

    /**
     * @param ctx Context.
     * @param log Log.
     * @param cacheName Cache name.
     * @param col Entries to put.
     * @param ignoreDepOwnership {@code True} to ignore deployment ownership.
     * @param skipStore Skip store flag.
     * @param rcvr Updater.
     */
    DataStreamerUpdateJob(
        GridKernalContext ctx,
        IgniteLogger log,
        @Nullable String cacheName,
        Collection<DataStreamerEntry> col,
        boolean ignoreDepOwnership,
        boolean skipStore,
        StreamReceiver<?, ?> rcvr) {
        this.ctx = ctx;
        this.log = log;

        assert col != null && !col.isEmpty();
        assert rcvr != null;

        this.cacheName = cacheName;
        this.col = col;
        this.ignoreDepOwnership = ignoreDepOwnership;
        this.skipStore = skipStore;
        this.rcvr = rcvr;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object call() throws Exception {
        if (log.isDebugEnabled())
            log.debug("Running put job [nodeId=" + ctx.localNodeId() + ", size=" + col.size() + ']');

        IgniteCacheProxy cache = ctx.cache().jcache(cacheName).cacheNoGate();

        cache.context().awaitStarted();

        if (skipStore)
            cache = (IgniteCacheProxy<?, ?>)cache.withSkipStore();

        if (ignoreDepOwnership)
            cache.context().deploy().ignoreOwnership(true);

        try {
            final GridCacheContext cctx = cache.context();

            for (DataStreamerEntry e : col) {
                e.getKey().finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

                CacheObject val = e.getValue();

                if (val != null) {
                    checkSecurityPermission(SecurityPermission.CACHE_PUT);

                    val.finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());
                }
                else
                    checkSecurityPermission(SecurityPermission.CACHE_REMOVE);
            }

            if (unwrapEntries()) {
                Collection<Map.Entry> col0 = F.viewReadOnly(col, new C1<DataStreamerEntry, Map.Entry>() {
                    @Override public Map.Entry apply(DataStreamerEntry e) {
                        return e.toEntry(cctx);
                    }
                });

                rcvr.receive(cache, col0);
            }
            else
                rcvr.receive(cache, col);

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
        return !(rcvr instanceof DataStreamerCacheUpdaters.InternalUpdater);
    }

    /**
     * @param perm Security permission.
     * @throws org.apache.ignite.plugin.security.SecurityException If permission is not enough.
     */
    private void checkSecurityPermission(SecurityPermission perm)
        throws org.apache.ignite.plugin.security.SecurityException {
        if (!ctx.security().enabled())
            return;

        ctx.security().authorize(cacheName, perm, null);
    }
}
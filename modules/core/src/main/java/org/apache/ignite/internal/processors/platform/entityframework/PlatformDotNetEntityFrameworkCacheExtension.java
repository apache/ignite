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

package org.apache.ignite.internal.processors.platform.entityframework;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheExtension;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessorResult;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * EntityFramework cache extension.
 */
@SuppressWarnings("unchecked")
public class PlatformDotNetEntityFrameworkCacheExtension implements PlatformCacheExtension {
    /** Extension ID. */
    private static final int EXT_ID = 1;

    /** Operation: increment entity set versions. */
    private static final int OP_INVALIDATE_SETS = 1;

    /** Cache key for cleanup node ID. Contains characters not allowed in SQL table name. */
    private static final String CLEANUP_NODE_ID = " ^CLEANUP_NODE_ID^ ";

    /** {@inheritDoc} */
    @Override public int id() {
        return EXT_ID;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public long processInOutStreamLong(PlatformCache target, int type, BinaryRawReaderEx reader,
        PlatformMemory mem) throws IgniteCheckedException {
        switch (type) {
            case OP_INVALIDATE_SETS: {
                final IgniteCache<String, Long> metaCache = (IgniteCache<String, Long>)target.rawCache();
                final String dataCacheName = reader.readString();

                int cnt = reader.readInt();

                assert cnt > 0;

                final Set<String> entitySetNames = new HashSet(cnt);

                for (int i = 0; i < cnt; i++)
                    entitySetNames.add(reader.readString());

                final Map<String, EntryProcessorResult<Long>> currentVersions =
                    metaCache.invokeAll(entitySetNames,
                    new PlatformDotNetEntityFrameworkIncreaseVersionProcessor());

                if (currentVersions.size() != cnt)
                {
                    throw new IgniteCheckedException("Failed to update entity set versions, expected: "
                        + cnt + ", actual: " + currentVersions.size());
                }

                Ignite grid = target.platformContext().kernalContext().grid();

                startBackgroundCleanup(grid, (IgniteCache<String, UUID>)(IgniteCache)metaCache,
                    dataCacheName, currentVersions);

                return target.writeResult(mem, null);
            }
        }

        throw new IgniteCheckedException("Unsupported operation type: " + type);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetEntityFrameworkCacheExtension.class, this);
    }

    /**
     * Starts the background cleanup of old cache entries.
     *
     * @param grid Grid.
     * @param metaCache Meta cache.
     * @param dataCacheName Data cache name.
     * @param currentVersions Current versions.
     */
    private void startBackgroundCleanup(Ignite grid, final Cache<String, UUID> metaCache,
        final String dataCacheName, final Map<String, EntryProcessorResult<Long>> currentVersions) {
        // Initiate old entries cleanup.
        IgniteCompute asyncCompute = grid.compute().withAsync();

        // Set a flag about running cleanup.
        final UUID localNodeId = grid.cluster().localNode().id();

        while (true) {
            if (metaCache.putIfAbsent(CLEANUP_NODE_ID, localNodeId))
                break;   // No cleanup is in progress, start new.

            // Some node is performing cleanup - check if it is alive.
            UUID nodeId = metaCache.get(CLEANUP_NODE_ID);

            if (nodeId == null)
                continue;  // Cleanup has stopped.

            if (nodeId.equals(localNodeId))
                return;  // Current node already performs cleanup.

            if (grid.cluster().node(nodeId) != null)
                return;  // Another node already performs cleanup and is alive.

            // Node that performs cleanup has disconnected.
            if (metaCache.replace(CLEANUP_NODE_ID, nodeId, localNodeId))
                break;  // Successfully replaced disconnected node id with our id.

            // Node id value has changed by another thread. Repeat the process.
        }

        asyncCompute.broadcast(new IgniteRunnable() {
            /** Inject Ignite. */
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public void run() {
                removeOldEntries(ignite, dataCacheName, currentVersions);
            }
        });

        asyncCompute.future().listen(new CI1<IgniteFuture<Object>>() {
            @Override public void apply(IgniteFuture<Object> future) {
                // Reset cleanup flag.
                metaCache.remove(CLEANUP_NODE_ID);
            }
        });
    }

    /**
     * Removes old cache entries locally.
     *
     * @param ignite Ignite.
     * @param dataCacheName Cache name.
     * @param currentVersions Current versions.
     */
    private static void removeOldEntries(final Ignite ignite, final String dataCacheName,
        final Map<String, EntryProcessorResult<Long>> currentVersions) {

        IgniteCache<String, PlatformDotNetEntityFrameworkCacheEntry> cache = ignite.cache(dataCacheName);

        for (Cache.Entry<String, PlatformDotNetEntityFrameworkCacheEntry> cacheEntry :
            cache.localEntries(CachePeekMode.ALL)) {
            PlatformDotNetEntityFrameworkCacheEntry entry = cacheEntry.getValue();

            for (Map.Entry<String, Long> entitySet : entry.entitySets().entrySet()) {
                EntryProcessorResult<Long> curVer = currentVersions.get(entitySet.getKey());

                if (curVer != null && entitySet.getValue() < curVer.get())
                    cache.remove(cacheEntry.getKey());
            }
        }
    }
}
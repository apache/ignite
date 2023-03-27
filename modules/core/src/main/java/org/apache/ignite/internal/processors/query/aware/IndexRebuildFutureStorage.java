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

package org.apache.ignite.internal.processors.query.aware;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptySet;

/**
 * Holder of up-to-date information of rebuild index futures for caches.
 * Thread safe.
 */
public class IndexRebuildFutureStorage {
    /**
     * Futures to track the status of index rebuilds.
     * Mapping: Cache id -> Future.
     * Guarded by {@code this}.
     */
    private final Map<Integer, GridFutureAdapter<Void>> futs = new HashMap<>();

    /**
     * Topology versions that will require index rebuilding on exchange.
     * Mapping: Cache id -> Topology version.
     * Guarded by {@code this}.
     */
    private final Map<Integer, AffinityTopologyVersion> tops = new HashMap<>();

    /**
     * Getting index rebuild future for the cache.
     *
     * @param cacheId Cache id.
     * @return Future if rebuilding is in progress or {@code null} if not.
     */
    @Nullable public synchronized GridFutureAdapter<Void> indexRebuildFuture(int cacheId) {
        return futs.get(cacheId);
    }

    /**
     * Checks that the indexes need to be rebuilt on the exchange.
     *
     * @param cacheId Cache id.
     * @param topVer Topology version.
     * @return {@code True} if need to rebuild.
     */
    public synchronized boolean rebuildIndexesOnExchange(int cacheId, AffinityTopologyVersion topVer) {
        boolean rebuild = tops.containsKey(cacheId) && topVer.equals(tops.get(cacheId));

        if (rebuild)
            assert futs.get(cacheId) != null;

        return rebuild;
    }

    /**
     * Canceling index rebuilding for caches on the exchange.
     *
     * @param cacheIds Cache ids.
     * @param topVer Topology version.
     */
    public void cancelRebuildIndexesOnExchange(Set<Integer> cacheIds, AffinityTopologyVersion topVer) {
        if (!cacheIds.isEmpty()) {
            synchronized (this) {
                for (Integer cacheId : cacheIds) {
                    if (tops.containsKey(cacheId) && topVer.equals(tops.get(cacheId)))
                        onFinishRebuildIndexes(cacheId, null);
                }
            }
        }
    }

    /**
     * Preparing futures of rebuilding indexes for caches.
     * The future for the cache will be added only if the previous one is missing or completed.
     *
     * @param cacheIds Cache ids.
     * @param topVer Topology version if rebuilding indexes should be on exchange.
     * @return Cache ids for which features have not been added.
     */
    public Set<Integer> prepareRebuildIndexes(Set<Integer> cacheIds, @Nullable AffinityTopologyVersion topVer) {
        if (!cacheIds.isEmpty()) {
            synchronized (this) {
                Set<Integer> alreadyPrepared = new HashSet<>();

                for (Integer cacheId : cacheIds) {
                    GridFutureAdapter<Void> prevFut = futs.get(cacheId);

                    if (prevFut == null || prevFut.isDone()) {
                        if (topVer != null) {
                            AffinityTopologyVersion prevTopVer = tops.put(cacheId, topVer);

                            assert prevTopVer == null;
                        }

                        futs.put(cacheId, new GridFutureAdapter<>());
                    }
                    else
                        alreadyPrepared.add(cacheId);
                }

                return alreadyPrepared;
            }
        }
        else
            return emptySet();
    }

    /**
     * Callback on finish of rebuilding indexes for the cache.
     *
     * @param cacheId Cache id.
     * @param err Error.
     */
    public synchronized void onFinishRebuildIndexes(int cacheId, @Nullable Throwable err) {
        tops.remove(cacheId);

        GridFutureAdapter<Void> rmv = futs.remove(cacheId);

        assert rmv != null;

        rmv.onDone(err);
    }
}



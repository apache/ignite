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

package org.apache.ignite.ml.dlc.impl.cache.util;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.ml.dlc.DLCPartition;
import org.apache.ignite.ml.dlc.DLCPartitionRecoverableTransformer;

/**
 * Distributed Learning Context partition builder which constructs a partition from two parts: replicated data which is
 * stored in a reliable Ignite Cache and recoverable data which is stored in a local storage and can be recovered from
 * the upstream cache.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
public class CacheBasedDLCPartitionBuilder<K, V, Q extends Serializable, W extends AutoCloseable> {
    /** Key used to store local partition storage in the {@link IgniteCluster#nodeLocalMap()}. */
    private static final String NODE_LOCAL_PART_STORAGE_KEY = "ml_local_part_storage";

    /** Ignite instance. */
    private final Ignite ignite;

    /** Upstream cache name. */
    private final String upstreamCacheName;

    /** Distributed Learning Context cache name. */
    private final String dlcCacheName;

    /** Distributed Learning Context cache name. */
    private final UUID dlcId;

    /** Partition index. */
    private final int partIdx;

    /**
     * Constructs a new instance of a cache based Distributed Learning Context partition builder.
     *
     * @param ignite ignite instance
     * @param upstreamCacheName upstream cache name
     * @param dlcCacheName distributed learning context cache name
     * @param dlcId distributed learning context id
     * @param partIdx partition index
     */
    public CacheBasedDLCPartitionBuilder(Ignite ignite, String upstreamCacheName, String dlcCacheName,
        UUID dlcId, int partIdx) {
        this.ignite = ignite;
        this.upstreamCacheName = upstreamCacheName;
        this.dlcCacheName = dlcCacheName;
        this.dlcId = dlcId;
        this.partIdx = partIdx;
    }

    /**
     * Builds a new instance of DLC partition constructed from the replicated and recoverable part. If it's required to
     * load recoverable data from the upstream cache but correspondent upstream cache partition is not presented on the
     * node the {@link UpstreamPartitionNotFoundException} will be thrown (with assumption that retry can be used in
     * this case).
     *
     * Be aware that this method works correctly only under the condition that partitions of the DLC cache and the
     * upstream cache are not moved during the execution. To guarantee this condition please use
     * {@link IgniteCompute#affinityCall(String, Object, IgniteCallable)} and
     * {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)} methods or similar to submit the job.
     *
     * @return distributed learning context partition
     */
    public DLCPartition<K, V, Q, W> build() {
        IgniteCache<Integer, DLCPartition<K, V, Q, W>> dlcCache = ignite.cache(dlcCacheName);

        // Retrieves partition without recoverable data (this data is not stored in DLC cache).
        DLCPartition<K, V, Q, W> part = dlcCache.get(partIdx);

        DLCPartitionRecoverableDataStorage<W> storage = getLocalPartitionStorage();

        W recoverableData = storage.getData().get(partIdx);

        // In case when partition just has been moved to the node and haven't been processed here yet recoverable data
        // will be empty and we need to load it from the upstream cache.
        if (recoverableData == null) {

            // Locks partition loading procedure to avoid multiple memory allocations for the same partition.
            Lock partLock = storage.getLocks().computeIfAbsent(partIdx, key -> new ReentrantLock());

            try {
                partLock.lock();

                // Loads recoverable data from the upstream cache.
                recoverableData = storage.getData().computeIfAbsent(
                    partIdx,
                    id -> loadRecoverableData(part.getRecoverableDataTransformer(), part.getReplicatedData())
                );
            }
            finally {
                partLock.unlock();
            }
        }

        part.setRecoverableData(recoverableData);

        return part;
    }

    /**
     * Loads recoverable data from the upstream cache.
     *
     * @param recoverableDataTransformer recoverable data transformer
     * @param replicatedData replicated data
     * @return recoverable data
     */
    private W loadRecoverableData(
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableDataTransformer,
        Q replicatedData) {
        Affinity<K> upstreamCacheAffinity = ignite.affinity(upstreamCacheName);
        ClusterNode partNode = upstreamCacheAffinity.mapPartitionToNode(partIdx);

        ClusterNode locNode = ignite.cluster().localNode();

        // If there is not required partition of the upstream cache on the node throws exception.
        if (!partNode.equals(locNode))
            throw new UpstreamPartitionNotFoundException(upstreamCacheName, locNode.id(), partIdx);

        IgniteCache<K, V> locUpstreamCache = ignite.cache(upstreamCacheName);

        ScanQuery<K, V> qry = new ScanQuery<>();
        qry.setLocal(true);
        qry.setPartition(partIdx);

        // TODO: how to guarantee that cache size will not be changed between these calls?
        long cnt = locUpstreamCache.localSizeLong(partIdx);
        try (QueryCursor<Cache.Entry<K, V>> cursor = locUpstreamCache.query(qry)) {
            return recoverableDataTransformer.transform(new DLCUpstreamCursorAdapter<>(cursor), cnt, replicatedData);
        }
    }

    /**
     * Retrieves partition recoverable data storage from the {@link IgniteCluster#nodeLocalMap()}.
     *
     * @return partition recoverable storage
     */
    @SuppressWarnings("unchecked")
    private DLCPartitionRecoverableDataStorage<W> getLocalPartitionStorage() {
        ConcurrentMap<String, Object> nodeLocMap = ignite.cluster().nodeLocalMap();

        ConcurrentMap<UUID, DLCPartitionRecoverableDataStorage<W>> locPartStorage =
            (ConcurrentMap<UUID, DLCPartitionRecoverableDataStorage<W>>)
                nodeLocMap.computeIfAbsent(NODE_LOCAL_PART_STORAGE_KEY, key -> new ConcurrentHashMap<>());

        return locPartStorage.computeIfAbsent(dlcId, key -> new DLCPartitionRecoverableDataStorage<>());
    }
}
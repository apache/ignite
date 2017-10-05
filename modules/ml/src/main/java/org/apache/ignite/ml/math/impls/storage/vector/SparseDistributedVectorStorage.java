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

package org.apache.ignite.ml.math.impls.storage.vector;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.DistributedStorage;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.distributed.keys.impl.SparseMatrixKey;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link VectorStorage} implementation for {@link /*SparseDistributedVector}.
 */
public class SparseDistributedVectorStorage extends CacheUtils implements VectorStorage, StorageConstants, DistributedStorage<Integer> {
    /** Cache name used for all instances of {@link SparseDistributedVectorStorage}. */
    private static final String CACHE_NAME = "ML_SPARSE_VECTORS_CONTAINER";
    /** Amount of elements in the vector. */
    private int size;
    /** Random or sequential access mode. */
    private int acsMode;
    /** Matrix uuid. */
    private IgniteUuid uuid;

    /** Actual distributed storage. */
    private IgniteCache<Integer, Double> cache = null;

    /**
     *
     */
    public SparseDistributedVectorStorage() {
        // No-op.
    }

    /**
     * @param size Amount of elements in the vector.
     * @param acsMode Random or sequential access mode.
     */
    public SparseDistributedVectorStorage(int size, int acsMode) {

        assert size > 0;
        assertAccessMode(acsMode);

        this.size = size;
        this.acsMode = acsMode;

        cache = newCache();

        uuid = IgniteUuid.randomUuid();
    }

    /**
     * Create new ML cache if needed.
     */
    private IgniteCache<Integer, Double> newCache() {
        CacheConfiguration<Integer, Double> cfg = new CacheConfiguration<>();

        // Write to primary.
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        // No eviction.
        cfg.setEvictionPolicy(null);

        // No copying of values.
        cfg.setCopyOnRead(false);

        // Cache is partitioned.
        cfg.setCacheMode(CacheMode.PARTITIONED);

        // Random cache name.
        cfg.setName(CACHE_NAME);

        return Ignition.localIgnite().getOrCreateCache(cfg);
    }

    /**
     *
     */
    public IgniteCache<Integer, Double> cache() {
        return cache;
    }

    public int accessMode() {
        return acsMode;
    }

    @Override
    public double get(int i) {
        // Remote get from the primary node (where given row or column is stored locally).
        return ignite().compute(getClusterGroupForGivenKey(CACHE_NAME, i)).call(() -> {
            IgniteCache<Integer, Double> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);
            return cache.get(i);
        });
    }

    @Override
    public void set(int i, double v) {
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(getClusterGroupForGivenKey(CACHE_NAME, i)).run(() -> {
            IgniteCache<Integer, Double> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);

            if (v != 0.0)
                cache.put(i, v);
            else if (cache.containsKey(i)) // remove zero elements
                cache.remove(i);

        });
    }


    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }


    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(acsMode);
        out.writeObject(uuid);
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        acsMode = in.readInt();
        uuid = (IgniteUuid)in.readObject();
        cache = ignite().getOrCreateCache(in.readUTF());
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return acsMode == SEQUENTIAL_ACCESS_MODE;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return acsMode == RANDOM_ACCESS_MODE;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** Delete all data from cache. */
    @Override public void destroy() {
        Set<Integer> keyset = IntStream.range(0, size).boxed().collect(Collectors.toSet());

        cache.clearAll(keyset);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + size;
        res = res * 37 + acsMode;
        res = res * 37 + uuid.hashCode();
        res = res * 37 + cache.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        SparseDistributedVectorStorage that = (SparseDistributedVectorStorage)obj;

        return size == that.size  && acsMode == that.acsMode
            && uuid.equals(that.uuid) && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> getAllKeys() {
        int range = size;

        return IntStream.range(0, range).boxed().collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return CACHE_NAME;
    }

    /** */
    public IgniteUuid getUUID() {
        return uuid;
    }



}

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

package org.apache.ignite.ml.math.impls.storage.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.DistributedStorage;
import org.apache.ignite.ml.math.distributed.keys.impl.VectorBlockKey;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.ml.math.impls.vector.VectorBlockEntry;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.ml.math.impls.matrix.MatrixBlockEntry.MAX_BLOCK_SIZE;

/**
 * Storage for {@link SparseBlockDistributedVector}.
 */
public class BlockVectorStorage extends CacheUtils implements VectorStorage, StorageConstants, DistributedStorage<VectorBlockKey> {
    /** Cache name used for all instances of {@link BlockVectorStorage}. */
    private static final String CACHE_NAME = "ML_BLOCK_SPARSE_MATRICES_CONTAINER";

    /** */
    private int blocks;

    /** Amount of columns in the vector. */
    private int size;

    /** Matrix uuid. */
    private UUID uuid;

    /** Block size about 8 KB of data. */
    private int maxBlockEdge = MAX_BLOCK_SIZE;

    /** Actual distributed storage. */
    private IgniteCache<
            VectorBlockKey /* Matrix block number with uuid. */,
            VectorBlockEntry /* Block of matrix, local sparse matrix. */
            > cache = null;

    /**
     *
     */
    public BlockVectorStorage() {
        // No-op.
    }

    /**
     * @param size Amount of columns in the vector.
     */
    public BlockVectorStorage(int size) {

        assert size > 0;

        this.size = size;
        this.blocks = size % maxBlockEdge == 0 ? size / maxBlockEdge : size / maxBlockEdge + 1;

        cache = newCache();
        uuid = UUID.randomUUID();
    }

    /** */
    public IgniteCache<VectorBlockKey, VectorBlockEntry> cache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public double get(int x) {
        return matrixGet(x);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, double v) {
        matrixSet(x, v);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(blocks);
        out.writeObject(uuid);
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        blocks = in.readInt();
        uuid = (UUID) in.readObject();

        cache = ignite().getOrCreateCache(in.readUTF());
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
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
        cache.clearAll(getAllKeys());
    }

    /**
     * Get storage UUID.
     *
     * @return storage UUID.
     */
    public UUID getUUID() {
        return uuid;
    }

    /**
     * Build the cache key for the given blocks id.
     *
     * NB: NOT cell indices.
     */
    public VectorBlockKey getCacheKey(long blockId) {
        return new VectorBlockKey(blockId, uuid, getAffinityKey(blockId));
    }


    /** {@inheritDoc} */
    @Override public Set<VectorBlockKey> getAllKeys() {
        int maxIdx = size - 1;
        long maxBlockId = getBlockId(maxIdx);

        Set<VectorBlockKey> keyset = new HashSet<>();

        for (int i = 0; i <= maxBlockId; i++)
            keyset.add(getCacheKey(i));

        return keyset;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return CACHE_NAME;
    }


    /**
     * Get column for current block.
     *
     * @param blockId block id.
     * @return The list of block entries.
     */
    public List<VectorBlockEntry> getColForBlock(long blockId) {
        List<VectorBlockEntry> res = new LinkedList<>();

        for (int i = 0; i < blocks; i++)
            res.add(getEntryById(i));

        return res;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + size;
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

        BlockVectorStorage that = (BlockVectorStorage) obj;

        return size == that.size && uuid.equals(that.uuid)
                && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }

    /**
     *
     */
    private VectorBlockEntry getEntryById(long blockId) {
        VectorBlockKey key = getCacheKey(blockId);

        VectorBlockEntry entry = cache.localPeek(key, CachePeekMode.PRIMARY);
        entry = entry != null ? entry : cache.get(key);

        if (entry == null)
            entry = getEmptyBlockEntry(blockId);

        return entry;
    }

    /**
     * Get empty block entry by the given block id.
     */
    @NotNull
    private VectorBlockEntry getEmptyBlockEntry(long blockId) {
        VectorBlockEntry entry;
        int colMod = size % maxBlockEdge;

        int colSize;

        if (colMod == 0)
            colSize = maxBlockEdge;
        else
            colSize = blockId != (blocks - 1) ? maxBlockEdge : colMod;

        entry = new VectorBlockEntry(colSize);
        return entry;
    }

    /**
     * TODO: IGNITE-5646, WIP
     *
     * Get affinity key for the given id.
     */
    private UUID getAffinityKey(long blockId) {
        return null;
    }

    /**
     * Distributed matrix set.
     *
     * @param idx Row or column index.
     * @param v   New value to set.
     */
    private void matrixSet(int idx, double v) {
        long blockId = getBlockId(idx);
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(getClusterGroupForGivenKey(CACHE_NAME, blockId)).run(() -> {
            IgniteCache<VectorBlockKey, VectorBlockEntry> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);

            VectorBlockKey key = getCacheKey(blockId);

            // Local get.
            VectorBlockEntry block = getEntryById(blockId);

            block.set(idx % block.size(), v);

            // Local put.
            cache.put(key, block);
        });
    }

    /** */
    private long getBlockId(int x) {
        return (long) x / maxBlockEdge;
    }

    /**
     * Distributed vector get.
     *
     * @param idx index.
     * @return Vector value at (idx) index.
     */
    private double matrixGet(int idx) {
        // Remote get from the primary node (where given row or column is stored locally).
        return ignite().compute(getClusterGroupForGivenKey(CACHE_NAME, getBlockId(idx))).call(() -> {
            IgniteCache<VectorBlockKey, VectorBlockEntry> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);

            VectorBlockKey key = getCacheKey(getBlockId(idx));

            // Local get.
            VectorBlockEntry block = cache.localPeek(key, CachePeekMode.PRIMARY);

            if (block == null)
                block = cache.get(key);

            return block == null ? 0.0 : block.get(idx % block.size());
        });
    }

    /**
     * Create new ML cache if needed.
     */
    private IgniteCache<VectorBlockKey, VectorBlockEntry> newCache() {
        CacheConfiguration<VectorBlockKey, VectorBlockEntry> cfg = new CacheConfiguration<>();

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
     * Avoid this method for large vectors
     *
     * @return data presented as array
     */
    @Override public double[] data() {
        double[] res = new double[this.size];
        for (int i = 0; i < this.size; i++) res[i] = this.get(i);
        return res;
    }
}
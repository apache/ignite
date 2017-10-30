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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.DistributedStorage;
import org.apache.ignite.ml.math.distributed.keys.impl.BlockMatrixKey;
import org.apache.ignite.ml.math.impls.matrix.BlockEntry;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;

import static org.apache.ignite.ml.math.impls.matrix.BlockEntry.MAX_BLOCK_SIZE;

/**
 * Storage for {@link SparseBlockDistributedMatrix}.
 */
public class BlockMatrixStorage extends CacheUtils implements MatrixStorage, StorageConstants, DistributedStorage<BlockMatrixKey> {
    /** Cache name used for all instances of {@link BlockMatrixStorage}. */
    private static final String CACHE_NAME = "ML_BLOCK_SPARSE_MATRICES_CONTAINER";
    /** */
    private int blocksInCol;
    /** */
    private int blocksInRow;
    /** Amount of rows in the matrix. */
    private int rows;
    /** Amount of columns in the matrix. */
    private int cols;
    /** Matrix uuid. */
    private IgniteUuid uuid;
    /** Block size about 8 KB of data. */
    private int maxBlockEdge = MAX_BLOCK_SIZE;

    /** Actual distributed storage. */
    private IgniteCache<
        BlockMatrixKey /* Matrix block number with uuid. */,
        BlockEntry /* Block of matrix, local sparse matrix. */
        > cache = null;

    /**
     *
     */
    public BlockMatrixStorage() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     */
    public BlockMatrixStorage(int rows, int cols) {
        assert rows > 0;
        assert cols > 0;

        this.rows = rows;
        this.cols = cols;

        this.blocksInRow = rows % maxBlockEdge == 0 ? rows / maxBlockEdge : rows / maxBlockEdge + 1;
        this.blocksInCol = cols % maxBlockEdge == 0 ? cols / maxBlockEdge : cols / maxBlockEdge + 1;

        cache = newCache();

        uuid = IgniteUuid.randomUuid();
    }

    /**
     *
     */
    public IgniteCache<BlockMatrixKey, BlockEntry> cache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return matrixGet(x, y);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        matrixSet(x, y, v);
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public int storageMode() {
        return UNKNOWN_STORAGE_MODE;
    }

    /** {@inheritDoc} */
    @Override public int accessMode() {
        return RANDOM_ACCESS_MODE;
    }

    /**
     * @return Blocks in column.
     */
    public int blocksInCol() {
        return blocksInCol;
    }

    /**
     * @return Blocks in row.
     */
    public int blocksInRow() {
        return blocksInRow;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        out.writeInt(blocksInRow);
        out.writeInt(blocksInCol);
        U.writeGridUuid(out, uuid);
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        blocksInRow = in.readInt();
        blocksInCol = in.readInt();
        uuid = U.readGridUuid(in);
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
    public IgniteUuid getUUID() {
        return uuid;
    }

    /**
     * Build the cache key for the given blocks id.
     *
     * NB: NOT cell indices.
     */
    public BlockMatrixKey getCacheKey(long blockIdRow, long blockIdCol) {
        return new BlockMatrixKey(blockIdRow, blockIdCol, uuid, getAffinityKey(blockIdRow, blockIdCol));
    }

    /**
     * Build the cache key for the given blocks id.
     *
     * NB: NOT cell indices.
     */
    public BlockMatrixKey getCacheKey(IgnitePair<Long> blockId) {
        return new BlockMatrixKey(blockId.get1(), blockId.get2(), uuid, getAffinityKey(blockId.get1(), blockId.get2()));
    }

    /** {@inheritDoc} */
    @Override public Set<BlockMatrixKey> getAllKeys() {
        IgnitePair<Long> maxBlockId = getBlockId(rows, cols);

        Set<BlockMatrixKey> keyset = new HashSet<>();

        for(int i = 0; i <= maxBlockId.get1(); i++)
            for(int j = 0; j <= maxBlockId.get2(); j++)
                keyset.add(getCacheKey(i,j));

        return keyset;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return CACHE_NAME;
    }

    /**
     * Get rows for current block.
     *
     * @param blockId block id.
     * @return The list of block entries.
     */
    public List<BlockEntry> getRowForBlock(IgnitePair<Long> blockId) {
        List<BlockEntry> res = new LinkedList<>();

        for (int i = 0; i < blocksInCol; i++)
            res.add(getEntryById(new IgnitePair<>((long) i, blockId.get2())));

        return res;
    }

    /**
     * Get cols for current block.
     *
     * @param blockId block id.
     * @return The list of block entries.
     */
    public List<BlockEntry> getColForBlock(IgnitePair<Long> blockId) {
        List<BlockEntry> res = new LinkedList<>();

        for (int i = 0; i < blocksInRow; i++)
            res.add(getEntryById(new IgnitePair<>(blockId.get1(), (long) i)));

        return res;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + cols;
        res = res * 37 + rows;
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

        BlockMatrixStorage that = (BlockMatrixStorage)obj;

        return rows == that.rows && cols == that.cols && uuid.equals(that.uuid)
            && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }

    /**
     *
     */
    private BlockEntry getEntryById(IgnitePair<Long> blockId) {
        BlockMatrixKey key = getCacheKey(blockId.get1(), blockId.get2());

        BlockEntry entry = cache.localPeek(key);
        entry = entry != null ? entry : cache.get(key);

        if (entry == null) {

            int colSize = blockId.get1() == (blocksInCol - 1) ? rows % maxBlockEdge : maxBlockEdge;
            int rowSize = blockId.get2() == (blocksInRow -1 ) ? cols % maxBlockEdge : maxBlockEdge;

            entry = new BlockEntry(rowSize, colSize);
        }

        return entry;
    }

    /**
     * TODO: IGNITE-5646, WIP
     *
     * Get affinity key for the given id.
     */
    private IgniteUuid getAffinityKey(long blockIdRow, long blockIdCol) {
        return null;
    }

    /**
     * Distributed matrix set.
     *
     * @param a Row or column index.
     * @param b Row or column index.
     * @param v New value to set.
     */
    private void matrixSet(int a, int b, double v) {
        IgnitePair<Long> blockId = getBlockId(a, b);
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(groupForKey(CACHE_NAME, blockId)).run(() -> {
            IgniteCache<BlockMatrixKey, BlockEntry> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);

            BlockMatrixKey key = getCacheKey(blockId.get1(), blockId.get2());

            // Local get.
            BlockEntry block = cache.localPeek(key, CachePeekMode.PRIMARY);

            if (block == null)
                block = cache.get(key); //Remote entry get.

            if (block == null)
                block = initBlockFor(a, b);

            block.set(a % block.rowSize(), b % block.columnSize(), v);

            // Local put.
            cache.put(key, block);
        });
    }

    /** */
    public IgnitePair<Long> getBlockId(int x, int y) {
        return new IgnitePair<>((long)x / maxBlockEdge, (long)y / maxBlockEdge);
    }

    /** */
    private BlockEntry initBlockFor(int x, int y) {
        int blockRows = 0;
        int blockCols = 0;

        if(rows >= maxBlockEdge)
            blockRows = rows - x >= maxBlockEdge ? maxBlockEdge : rows - x;
        else
            blockRows =  rows;

        if(cols >= maxBlockEdge)
            blockCols = cols - y >= maxBlockEdge ? maxBlockEdge : cols - y;
        else
            blockCols = cols;

        return new BlockEntry(blockRows, blockCols);
    }

    /**
     * Distributed matrix get.
     *
     * @param a Row or column index.
     * @param b Row or column index.
     * @return Matrix value at (a, b) index.
     */
    private double matrixGet(int a, int b) {
        // Remote get from the primary node (where given row or column is stored locally).
        return ignite().compute(groupForKey(CACHE_NAME, getBlockId(a, b))).call(() -> {
            IgniteCache<BlockMatrixKey, BlockEntry> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);

            BlockMatrixKey key = getCacheKey(getBlockId(a, b));

            // Local get.
            BlockEntry block = cache.localPeek(key, CachePeekMode.PRIMARY);

            if (block == null)
                block = cache.get(key);

            return block == null ? 0.0 : block.get(a % block.rowSize(), b % block.columnSize());
        });
    }

    /**
     * Create new ML cache if needed.
     */
    private IgniteCache<BlockMatrixKey, BlockEntry> newCache() {
        CacheConfiguration<BlockMatrixKey, BlockEntry> cfg = new CacheConfiguration<>();

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
}

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
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
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

        //cols % maxBlockEdge > 0 ? 1 : 0

        this.blocksInRow = cols % maxBlockEdge == 0 ? cols / maxBlockEdge : cols / maxBlockEdge + 1;
        this.blocksInCol = rows % maxBlockEdge == 0 ? rows / maxBlockEdge : rows / maxBlockEdge + 1;

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
        long maxBlockId = getBlockId(cols, rows);

        Set<BlockMatrixKey> keyset = LongStream.rangeClosed(0, maxBlockId).mapToObj(this::getCacheKey).collect(Collectors.toSet());

        cache.clearAll(keyset);
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
     * Build the cache key for the given block id
     */
    public BlockMatrixKey getCacheKey(long blockId) {
        return new BlockMatrixKey(blockId, uuid, getAffinityKey(blockId));
    }

    /**
     * Get rows for current block.
     *
     * @param blockId block id.
     * @param storageC result storage.
     * @return The list of block entries.
     */
    public List<BlockEntry> getRowForBlock(long blockId, BlockMatrixStorage storageC) {
        long blockRow = blockId / storageC.blocksInCol;
        long blockCol = blockId % storageC.blocksInRow;

        long locBlock = this.blocksInRow * (blockRow) + (blockCol >= this.blocksInRow ? (blocksInRow - 1) : blockCol);

        return getRowForBlock(locBlock);
    }

    /**
     * Get cols for current block.
     *
     * @param blockId block id.
     * @param storageC result storage.
     * @return The list of block entries.
     */
    public List<BlockEntry> getColForBlock(long blockId, BlockMatrixStorage storageC) {
        long blockRow = blockId / storageC.blocksInCol;
        long blockCol = blockId % storageC.blocksInRow;

        long locBlock = this.blocksInRow * (blockRow) + (blockCol >= this.blocksInRow ? (blocksInRow - 1) : blockCol);

        return getColForBlock(locBlock);
    }

    /** {@inheritDoc} */
    @Override public Set<BlockMatrixKey> getAllKeys() {
        long maxBlockId = numberOfBlocks();
        Set<BlockMatrixKey> keys = new HashSet<>();

        for (long id = 0; id < maxBlockId; id++)
            keys.add(getCacheKey(id));

        return keys;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return CACHE_NAME;
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

    /** */
    private List<BlockEntry> getRowForBlock(long blockId) {
        List<BlockEntry> res = new LinkedList<>();

        boolean isFirstRow = blockId < blocksInRow;

        long startBlock = isFirstRow ? 0 : blockId - blockId % blocksInRow;
        long endBlock = startBlock + blocksInRow - 1;

        for (long i = startBlock; i <= endBlock; i++)
            res.add(getEntryById(i));

        return res;
    }

    /** */
    private List<BlockEntry> getColForBlock(long blockId) {
        List<BlockEntry> res = new LinkedList<>();

        long startBlock = blockId % blocksInRow;
        long endBlock = startBlock + blocksInRow * (blocksInCol - 1);

        for (long i = startBlock; i <= endBlock; i += blocksInRow)
            res.add(getEntryById(i));

        return res;
    }

    /**
     *
     */
    private BlockEntry getEntryById(long blockId) {
        BlockMatrixKey key = getCacheKey(blockId);

        BlockEntry entry = cache.localPeek(key);
        entry = entry != null ? entry : cache.get(key);

        if (entry == null) {
            long colId = blockId == 0 ? 0 : blockId + 1;

            boolean isLastRow = (blockId) >= blocksInRow * (blocksInCol - 1);
            boolean isLastCol = (colId) % blocksInRow == 0;

            entry = new BlockEntry(isLastRow && rows % maxBlockEdge != 0 ? rows % maxBlockEdge : maxBlockEdge, isLastCol && cols % maxBlockEdge != 0 ? cols % maxBlockEdge : maxBlockEdge);
        }

        return entry;
    }

    /**
     *
     */
    private long numberOfBlocks() {
        int rows = rowSize();
        int cols = columnSize();

        return ((rows / maxBlockEdge) + (((rows % maxBlockEdge) > 0) ? 1 : 0))
            * ((cols / maxBlockEdge) + (((cols % maxBlockEdge) > 0) ? 1 : 0));
    }

    /**
     * TODO: IGNITE-5646, WIP
     *
     * Get affinity key for the given id.
     */
    private IgniteUuid getAffinityKey(long id) {
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
        long id = getBlockId(a, b);
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(groupForKey(CACHE_NAME, id)).run(() -> {
            IgniteCache<BlockMatrixKey, BlockEntry> cache = Ignition.localIgnite().getOrCreateCache(CACHE_NAME);

            BlockMatrixKey key = getCacheKey(getBlockId(a, b));

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
    private long getBlockId(int x, int y) {
        return (y / maxBlockEdge) * blockShift(cols) + (x / maxBlockEdge);
    }

    /** */
    private BlockEntry initBlockFor(int x, int y) {
        int blockRows = rows - x >= maxBlockEdge ? maxBlockEdge : rows - x;
        int blockCols = cols - y >= maxBlockEdge ? maxBlockEdge : cols - y;

        return new BlockEntry(blockRows, blockCols);
    }

    /** */
    private int blockShift(int i) {
        return (i) / maxBlockEdge + ((i) % maxBlockEdge > 0 ? 1 : 0);
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

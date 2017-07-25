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

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.CacheUtils;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;

/**
 * {@link MatrixStorage} implementation for {@link SparseDistributedMatrix}.
 */
public class SparseDistributedMatrixStorage extends CacheUtils implements MatrixStorage, StorageConstants {
    /** Cache name used for all instances of {@link SparseDistributedMatrixStorage}. */
    public static final String ML_CACHE_NAME = "ML_SPARSE_MATRICES_CONTAINER";
    /** Amount of rows in the matrix. */
    private int rows;
    /** Amount of columns in the matrix. */
    private int cols;
    /** Row or column based storage mode. */
    private int stoMode;
    /** Random or sequential access mode. */
    private int acsMode;
    /** Matrix uuid. */
    private IgniteUuid uuid;

    /** Actual distributed storage. */
    private IgniteCache<
        IgniteBiTuple<Integer, IgniteUuid> /* Row or column index with matrix uuid. */,
        Map<Integer, Double> /* Map-based row or column. */
        > cache = null;

    /**
     *
     */
    public SparseDistributedMatrixStorage() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     * @param stoMode Row or column based storage mode.
     * @param acsMode Random or sequential access mode.
     */
    public SparseDistributedMatrixStorage(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        this.rows = rows;
        this.cols = cols;
        this.stoMode = stoMode;
        this.acsMode = acsMode;

        cache = newCache();

        uuid = IgniteUuid.randomUuid();
    }

    /**
     * Create new ML cache if needed.
     */
    private IgniteCache<IgniteBiTuple<Integer, IgniteUuid>, Map<Integer, Double>> newCache() {
        CacheConfiguration<IgniteBiTuple<Integer, IgniteUuid>, Map<Integer, Double>> cfg = new CacheConfiguration<>();

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
        cfg.setName(ML_CACHE_NAME);

        return Ignition.localIgnite().getOrCreateCache(cfg);
    }

    /**
     *
     *
     */
    public IgniteCache<IgniteBiTuple<Integer, IgniteUuid>, Map<Integer, Double>> cache() {
        return cache;
    }

    /**
     *
     *
     */
    public int accessMode() {
        return acsMode;
    }

    /**
     *
     *
     */
    public int storageMode() {
        return stoMode;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        if (stoMode == ROW_STORAGE_MODE)
            return matrixGet(x, y);
        else
            return matrixGet(y, x);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        if (stoMode == ROW_STORAGE_MODE)
            matrixSet(x, y, v);
        else
            matrixSet(y, x, v);
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
        return ignite().compute(groupForKey(ML_CACHE_NAME, a)).call(() -> {
            IgniteCache<IgniteBiTuple<Integer, IgniteUuid>, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(ML_CACHE_NAME);

            // Local get.
            Map<Integer, Double> map = cache.localPeek(getCacheKey(a), CachePeekMode.PRIMARY);

            if (map == null)
                map = cache.get(getCacheKey(a));

            return (map == null || !map.containsKey(b)) ? 0.0 : map.get(b);
        });
    }

    /**
     * Distributed matrix set.
     *
     * @param a Row or column index.
     * @param b Row or column index.
     * @param v New value to set.
     */
    private void matrixSet(int a, int b, double v) {
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(groupForKey(ML_CACHE_NAME, a)).run(() -> {
            IgniteCache<IgniteBiTuple<Integer, IgniteUuid>, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(ML_CACHE_NAME);

            // Local get.
            Map<Integer, Double> map = cache.localPeek(getCacheKey(a), CachePeekMode.PRIMARY);

            if (map == null) {
                map = cache.get(getCacheKey(a)); //Remote entry get.

                if (map == null)
                    map = acsMode == SEQUENTIAL_ACCESS_MODE ? new Int2DoubleRBTreeMap() : new Int2DoubleOpenHashMap();
            }

            if (v != 0.0)
                map.put(b, v);
            else if (map.containsKey(b))
                map.remove(b);

            // Local put.
            cache.put(getCacheKey(a), map);
        });
    }

    /** Build cache key for row/column. */
    private IgniteBiTuple<Integer, IgniteUuid> getCacheKey(int idx) {
        return new IgniteBiTuple<>(idx, uuid);
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        out.writeInt(acsMode);
        out.writeInt(stoMode);
        out.writeObject(uuid);
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        acsMode = in.readInt();
        stoMode = in.readInt();
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
        Set<IgniteBiTuple<Integer, IgniteUuid>> keyset = IntStream.range(0, rows).mapToObj(this::getCacheKey).collect(Collectors.toSet());

        cache.clearAll(keyset);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + cols;
        res = res * 37 + rows;
        res = res * 37 + acsMode;
        res = res * 37 + stoMode;
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

        SparseDistributedMatrixStorage that = (SparseDistributedMatrixStorage)obj;

        return rows == that.rows && cols == that.cols && acsMode == that.acsMode && stoMode == that.stoMode
            && uuid.equals(that.uuid) && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }

    /** */
    public IgniteUuid getUUID() {
        return uuid;
    }
}

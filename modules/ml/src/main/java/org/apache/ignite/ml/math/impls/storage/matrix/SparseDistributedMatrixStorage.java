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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.CacheUtils;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;

/**
 * {@link MatrixStorage} implementation for {@link SparseDistributedMatrix}.
 */
public class SparseDistributedMatrixStorage extends CacheUtils implements MatrixStorage, StorageConstants {
    /** Amount of rows in the matrix. */
    private int rows;
    /** Amount of columns in the matrix. */
    private int cols;

    /** Row or column based storage mode. */
    private int stoMode;
    /** Random or sequential access mode. */
    private int acsMode;

    /** Actual distributed storage. */
    private IgniteCache<
        Integer /* Row or column index. */,
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
    }

    /**
     *
     *
     */
    private IgniteCache<Integer, Map<Integer, Double>> newCache() {
        CacheConfiguration<Integer, Map<Integer, Double>> cfg = new CacheConfiguration<>();

        // Assume 10% density.
        cfg.setStartSize(Math.max(1024, (rows * cols) / 10));

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
        cfg.setName(new IgniteUuid().shortString());

        return Ignition.localIgnite().getOrCreateCache(cfg);
    }

    /**
     *
     *
     */
    public IgniteCache<Integer, Map<Integer, Double>> cache() {
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
            return matrixGet(cache.getName(), x, y);
        else
            return matrixGet(cache.getName(), y, x);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        if (stoMode == ROW_STORAGE_MODE)
            matrixSet(cache.getName(), x, y, v);
        else
            matrixSet(cache.getName(), y, x, v);
    }

    /**
     * Distributed matrix get.
     *
     * @param cacheName Matrix's cache.
     * @param a Row or column index.
     * @param b Row or column index.
     * @return Matrix value at (a, b) index.
     */
    private double matrixGet(String cacheName, int a, int b) {
        // Remote get from the primary node (where given row or column is stored locally).
        return ignite().compute(groupForKey(cacheName, a)).call(() -> {
            IgniteCache<Integer, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

            // Local get.
            Map<Integer, Double> map = cache.localPeek(a, CachePeekMode.PRIMARY);

            return (map == null || !map.containsKey(b)) ? 0.0 : map.get(b);
        });
    }

    /**
     * Distributed matrix set.
     *
     * @param cacheName Matrix's cache.
     * @param a Row or column index.
     * @param b Row or column index.
     * @param v New value to set.
     */
    private void matrixSet(String cacheName, int a, int b, double v) {
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(groupForKey(cacheName, a)).run(() -> {
            IgniteCache<Integer, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

            // Local get.
            Map<Integer, Double> map = cache.localPeek(a, CachePeekMode.PRIMARY);

            if (map == null)
                map = acsMode == SEQUENTIAL_ACCESS_MODE ? new Int2DoubleRBTreeMap() : new Int2DoubleOpenHashMap();

            if (v != 0.0)
                map.put(b, v);
            else if (map.containsKey(b))
                map.remove(b);

            // Local put.
            cache.put(a, map);
        });
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
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        acsMode = in.readInt();
        stoMode = in.readInt();
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

    /** Destroy underlying cache. */
    @Override public void destroy() {
        cache.destroy();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + cols;
        res = res * 37 + rows;
        res = res * 37 + acsMode;
        res = res * 37 + stoMode;
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
            && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }
}

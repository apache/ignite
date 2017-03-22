// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.math.impls.storage.matrix;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * TODO: add description.
 */
public class SparseDistributedMatrixStorage extends CacheSupport implements MatrixStorage, StorageConstants {
    private int rows, cols;
    private int stoMode, acsMode;

    // Actual distributed storage.
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
     *
     * @param rows
     * @param cols
     * @param stoMode
     * @param acsMode
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
     * @return
     */
    private IgniteCache<Integer, Map<Integer, Double>> newCache() {
        CacheConfiguration<Integer, Map<Integer, Double>> cfg = new CacheConfiguration<>();

        // Assume 10% density.
        cfg.setStartSize(Math.max(1024, (rows * cols) / 10));

        // Write to primary.
        cfg.setAtomicWriteOrderMode(PRIMARY);
        cfg.setWriteSynchronizationMode(PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(ATOMIC);

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
     * @return
     */
    public IgniteCache<Integer, Map<Integer, Double>> cache() {
        return cache;
    }

    /**
     *
     * @return
     */
    public int accessMode() {
        return acsMode;
    }

    /**
     * 
     * @return
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

            map.put(b, v);

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
}

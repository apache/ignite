// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.math.impls.storage.matrix;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.math.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * TODO: add description.
 */
public class SparseDistributedMatrixStorage implements MatrixStorage, StorageConstants {
    private int rows, cols;
    private int stoMode, acsMode;

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

        // Cache is partitioned.
        cfg.setCacheMode(CacheMode.PARTITIONED);

        cache = Ignition.ignite().getOrCreateCache(cfg);
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

    @Override
    public double get(int x, int y) {
        return 0; // TODO
    }

    @Override
    public void set(int x, int y, double v) {
        // TODO
    }

    @Override
    public int columnSize() {
        return cols;
    }

    @Override
    public int rowSize() {
        return rows;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
    }

    @Override
    public boolean isSequentialAccess() {
        return acsMode == SEQUENTIAL_ACCESS_MODE;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public boolean isRandomAccess() {
        return true;
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public boolean isArrayBased() {
        return false; 
    }
}

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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;

/**
 * Storage for sparse, local, on-heap matrix.
 */
public class SparseLocalOnHeapMatrixStorage implements MatrixStorage, StorageConstants {
    /** Default zero value. */
    private static final double DEFAULT_VALUE = 0.0;

    /** */
    private int rows;
    /** */
    private int cols;

    /** */
    private int acsMode;
    /** */
    private int stoMode;

    /** Actual map storage. */
    private Map<Integer, Map<Integer, Double>> sto;

    /** */
    public SparseLocalOnHeapMatrixStorage() {
        // No-op.
    }

    /** */
    public SparseLocalOnHeapMatrixStorage(int rows, int cols, int acsMode, int stoMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        this.rows = rows;
        this.cols = cols;
        this.acsMode = acsMode;
        this.stoMode = stoMode;

        sto = new HashMap<>();
    }

    /**
     * @return Matrix elements storage mode.
     */
    public int getStorageMode() {
        return stoMode;
    }

    /**
     * @return Matrix elements access mode.
     */
    public int getAccessMode() {
        return acsMode;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        if (stoMode == ROW_STORAGE_MODE) {
            Map<Integer, Double> row = sto.get(x);

            if (row != null) {
                Double val = row.get(y);

                if (val != null)
                    return val;
            }

            return DEFAULT_VALUE;
        }
        else {
            Map<Integer, Double> col = sto.get(y);

            if (col != null) {
                Double val = col.get(x);

                if (val != null)
                    return val;
            }

            return DEFAULT_VALUE;
        }
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        // Ignore default values (currently 0.0).
        if (v != DEFAULT_VALUE) {
            if (stoMode == ROW_STORAGE_MODE) {
                Map<Integer, Double> row = sto.computeIfAbsent(x, k ->
                    acsMode == SEQUENTIAL_ACCESS_MODE ? new Int2DoubleRBTreeMap() : new Int2DoubleOpenHashMap());

                row.put(y, v);
            }
            else {
                Map<Integer, Double> col = sto.computeIfAbsent(y, k ->
                    acsMode == SEQUENTIAL_ACCESS_MODE ? new Int2DoubleRBTreeMap() : new Int2DoubleOpenHashMap());

                col.put(x, v);
            }
        }
        else {
            if (stoMode == ROW_STORAGE_MODE) {
                if (sto.containsKey(x)) {
                    Map<Integer, Double> row = sto.get(x);

                    if (row.containsKey(y))
                        row.remove(y);
                }

            }
            else {
                if (sto.containsKey(y)) {
                    Map<Integer, Double> col = sto.get(y);

                    if (col.containsKey(x))
                        col.remove(x);
                }
            }
        }
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
        out.writeObject(sto);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        acsMode = in.readInt();
        stoMode = in.readInt();
        sto = (Map<Integer, Map<Integer, Double>>)in.readObject();
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
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + rows;
        res = res * 37 + cols;
        res = res * 37 + sto.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SparseLocalOnHeapMatrixStorage that = (SparseLocalOnHeapMatrixStorage)o;

        return rows == that.rows && cols == that.cols && acsMode == that.acsMode && stoMode == that.stoMode
            && (sto != null ? sto.equals(that.sto) : that.sto == null);
    }
}

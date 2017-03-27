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

package org.apache.ignite.math.impls.storage.matrix;

import it.unimi.dsi.fastutil.ints.*;
import java.io.*;
import java.util.*;
import org.apache.ignite.math.StorageConstants;
import org.apache.ignite.math.MatrixStorage;

/**
 * Storage for sparse, local, on-heap matrix.
 */
public class SparseLocalOnHeapMatrixStorage implements MatrixStorage, StorageConstants {
    private int rows, cols;
    private int acsMode, stoMode;

    // Actual map storage.
    private Map<Integer, Map<Integer, Double>> sto;

    /**
     *
     */
    public SparseLocalOnHeapMatrixStorage() {
        // No-op.
    }

    public SparseLocalOnHeapMatrixStorage(int rows, int cols, int acsMode, int stoMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        this.rows = rows;
        this.cols = cols;
        this.acsMode = acsMode;
        this.stoMode = stoMode;

        sto = new HashMap<Integer, Map<Integer, Double>>();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SparseLocalOnHeapMatrixStorage that = (SparseLocalOnHeapMatrixStorage) o;

        return rows == that.rows && cols == that.cols && acsMode == that.acsMode && stoMode == that.stoMode
            && (sto != null ? sto.equals(that.sto) : that.sto == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + rows;
        res = res * 37 + cols;
        res = res * 37 + sto.hashCode();

        return res;
    }

    /**
     *
     * @return
     */
    public int getStorageMode() {
        return stoMode;
    }

    /**
     *
     * @return
     */
    public int getAccessMode() {
        return acsMode;
    }

    @Override public double get(int x, int y) {
        if (stoMode == ROW_STORAGE_MODE) {
            Map<Integer, Double> row = sto.get(x);

            if (row != null) {
                Double val = row.get(y);

                if (val != null)
                    return val;
            }

            return 0.0;
        }
        else {
            Map<Integer, Double> col = sto.get(y);

            if (col != null) {
                Double val = col.get(x);

                if (val != null)
                    return val;
            }

            return 0.0;
        }
    }

    @Override public void set(int x, int y, double v) {
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

    @Override public int columnSize() {
        return cols;
    }

    @Override public int rowSize() {
        return rows;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        out.writeInt(acsMode);
        out.writeInt(stoMode);
        out.writeObject(sto);
    }

    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        acsMode = in.readInt();
        stoMode = in.readInt();
        sto = (Map<Integer, Map<Integer, Double>>)in.readObject();
    }

    @Override public boolean isSequentialAccess() {
        return acsMode == SEQUENTIAL_ACCESS_MODE;
    }

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

    @Override public boolean isArrayBased() {
        return false;
    }
}

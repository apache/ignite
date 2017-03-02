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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.math.MatrixStorage;
import org.apache.ignite.math.VectorStorage;
import org.apache.ignite.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.math.impls.storage.vector.RandomAccessSparseVectorStorage;
import org.apache.ignite.math.impls.storage.vector.SequentialAccessSparseVectorStorage;

/**
 * Storage for sparse local matrix.
 */
public class SparseLocalMatrixStorage implements MatrixStorage {
    private Int2ObjectOpenHashMap<VectorStorage> rowVectors;

    private int rows;

    private int cols;

    /**
     * 0 - sequential access mode, 1 - random access mode.
     *
     * Random access mode is default mode.
     */
    private int accessMode = SparseDistributedMatrix.RANDOM_ACCESS_MODE;

    /** For serialization. */
    public SparseLocalMatrixStorage(){
        // No-op.
    }

    /**
     * Create empty matrix with given dimensions.
     *
     * @param rows Rows.
     * @param cols Columns.
     */
    public SparseLocalMatrixStorage(int rows, int cols, int accessMode) {
        this.rows = rows;
        this.cols = cols;
        this.accessMode = accessMode;

        initDataStorage();
    }

    /**
     * Create new Sparse local matrix from given 2d array.
     *
     * @param data Data.
     */
    public SparseLocalMatrixStorage(double[][] data) {
        this(data.length, data[0].length, SparseDistributedMatrix.RANDOM_ACCESS_MODE);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                set(i, j, data[i][j]);
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return rowVectors.get(x).get(y);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        VectorStorage row = rowVectors.get(x);
        row.set(y, v);
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
        out.writeInt(accessMode);

        out.writeObject(rowVectors);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        accessMode = in.readInt();

        rowVectors = (Int2ObjectOpenHashMap<VectorStorage>) in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return accessMode == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass() && compareStorage((SparseLocalMatrixStorage)obj);
    }

    private boolean compareStorage(SparseLocalMatrixStorage obj) {
        return  (rows == obj.rows) && (cols == obj.cols) && (obj.rowVectors.equals(rowVectors));
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + cols;
        result = result * 37 + rows;
        result = result * 37 + accessMode;
        result = result * 37 + rowVectors.hashCode();

        return result;
    }

    /**
     * Init all row objects.
     */
    private void initDataStorage(){
        rowVectors = new Int2ObjectOpenHashMap();

        for (int i = 0; i < rows; i++)
            rowVectors.put(i, selectStorage(cols));
    }

    /** */
    private VectorStorage selectStorage(int size){
        switch (accessMode){
            case 0:
                return new SequentialAccessSparseVectorStorage();
            case 1:
                return new RandomAccessSparseVectorStorage(size);
            default:
                throw new java.lang.UnsupportedOperationException("This access mode is unsupported.");
        }
    }
}

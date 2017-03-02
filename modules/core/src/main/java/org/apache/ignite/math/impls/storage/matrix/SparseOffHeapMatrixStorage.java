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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.math.MatrixStorage;

/**
 * Off heap storage for sparse matrix.
 */
public class SparseOffHeapMatrixStorage implements MatrixStorage {
    /** */
    private int rows;
    /** */
    private int cols;
    /** */
    private long ptr;

    /** */
    public SparseOffHeapMatrixStorage(){
        // No-op.
    }

    /** */
    public SparseOffHeapMatrixStorage(int rows, int cols){
        this.rows = rows;
        this.cols = cols;

        allocateMemory(rows, cols);
    }

    /** */
    public SparseOffHeapMatrixStorage(double[][] data) {
        this.rows = data.length;
        this.cols = data[0].length;

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                set(i,j,data[i][j]);
    }


    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return GridUnsafe.getDouble(pointerOffset(x, y));
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        GridUnsafe.putDouble(pointerOffset(x, y), v);
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                out.writeDouble(get(i, j));
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();

        allocateMemory(rows, cols);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                set(i, j, in.readDouble());
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        GridUnsafe.freeMemory(ptr);
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
    @Override public double getLookupCost() {
        return 0;
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
        return obj != null &&
            getClass().equals(obj.getClass()) &&
            (rows == ((SparseOffHeapMatrixStorage)obj).rows) &&
            (cols == ((SparseOffHeapMatrixStorage)obj).cols) &&
            (rows == 0 || cols == 0 || ptr == ((SparseOffHeapMatrixStorage)obj).ptr || isMemoryEquals((SparseOffHeapMatrixStorage)obj));
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + rows;
        result = result * 37 + cols;
        result = result * 37 + Long.hashCode(ptr);

        return result;
    }

    /** */
    private boolean isMemoryEquals(SparseOffHeapMatrixStorage otherStorage){
        boolean result = true;
        for (int i = 0; i < otherStorage.rows; i++) {
            for (int j = 0; j < otherStorage.cols; j++) {
                if (Double.compare(get(i,j),otherStorage.get(i,j)) != 0){
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    /** */
    private void allocateMemory(int rows, int cols) {
        ptr = GridUnsafe.allocateMemory(rows * cols * Double.BYTES);
    }

    /**
     * Calculate pointer for the specific row and column
     *
     * @param x Row.
     * @param y Column.
     * @return Matrix element pointer.
     */
    private long pointerOffset(int x, int y) {
        return ptr + x * cols * Double.BYTES + y * Double.BYTES;
    }
}

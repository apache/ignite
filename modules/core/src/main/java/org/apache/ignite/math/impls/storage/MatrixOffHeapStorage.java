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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.internal.util.*;
import org.apache.ignite.math.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class MatrixOffHeapStorage implements MatrixStorage {
    private int rows;
    private int cols;
    private long ptr;

    /** */
    public MatrixOffHeapStorage(){
        // No-op.
    }

    /** */
    public MatrixOffHeapStorage(int rows, int cols){
        this.rows = rows;

        this.cols = cols;

        ptr = GridUnsafe.allocateMemory(rows * cols * Double.BYTES);
    }

    /** */
    public MatrixOffHeapStorage(double[][] data) {
        this.rows = data.length;
        this.cols = data[0].length;

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                set(i,j,data[i][j]);
    }

    /** {@inheritDoc} */
    @Override
    public double get(int x, int y) {
        return GridUnsafe.getDouble(pointerOffset(x, y));
    }

    /** {@inheritDoc} */
    @Override
    public void set(int x, int y, double v) {
        GridUnsafe.putDouble(pointerOffset(x, y), v);
    }

    @Override
    public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSequentialAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public double getLookupCost() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAddConstantTime() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public double[][] data() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(ptr);
        out.writeInt(rows);
        out.writeInt(cols);
    }

    /** {@inheritDoc} */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ptr = in.readLong();
        rows = in.readInt();
        cols = in.readInt();
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        GridUnsafe.freeMemory(ptr);
    }

    /** {@inheritDoc} */
    private long pointerOffset(int x, int y){
        return ptr + x * cols + y;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        return obj != null &&
                getClass().equals(obj.getClass()) &&
                (rows == ((MatrixOffHeapStorage)obj).rows) &&
                (cols == ((MatrixOffHeapStorage)obj).cols) &&
                (rows == 0 || cols == 0 || ptr == ((MatrixOffHeapStorage)obj).ptr || isMemoryEquals((MatrixOffHeapStorage)obj));
    }

    /** */
    private boolean isMemoryEquals(MatrixOffHeapStorage otherStorage){
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
}

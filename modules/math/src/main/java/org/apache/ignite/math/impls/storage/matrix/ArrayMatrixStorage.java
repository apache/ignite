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

import java.io.*;
import java.util.*;
import org.apache.ignite.math.MatrixStorage;

/**
 * TODO: add description.
 */
public class ArrayMatrixStorage implements MatrixStorage {
    private double[][] data;
    private int rows, cols;

    /**
     *
     */
    public ArrayMatrixStorage() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     */
    public ArrayMatrixStorage(int rows, int cols) {
        assert rows > 0;
        assert cols > 0;

        this.data = new double[rows][cols];
        this.rows = rows;
        this.cols = cols;
    }

    /**
     *
     * @param data
     */
    public ArrayMatrixStorage(double[][] data) {
        assert data != null;
        assert data[0] != null;

        this.data = data;
        this.rows = data.length;
        this.cols = data[0].length;

        assert rows > 0;
        assert cols > 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass())
            && Arrays.deepEquals(data, ((ArrayMatrixStorage)o).data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + rows;
        result = result * 37 + cols;
        result = result * 37 + Arrays.deepHashCode(data);

        return result;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return data[x][y];
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        data[x][y] = v;
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
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public double[][] data() {
        return data;
    }

    /** {@inheritDoc */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
    }

    /** {@inheritDoc */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (double[][])in.readObject();
    }
}

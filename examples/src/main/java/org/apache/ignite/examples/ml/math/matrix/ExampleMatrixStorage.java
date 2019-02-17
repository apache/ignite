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

package org.apache.ignite.examples.ml.math.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.storage.matrix.ArrayMatrixStorage;
import org.apache.ignite.ml.math.util.MatrixUtil;

/**
 * Example matrix storage implementation, modeled after {@link ArrayMatrixStorage}.
 */
class ExampleMatrixStorage implements MatrixStorage {
    /** Backing data array. */
    private double[][] data;
    /** Amount of rows in a matrix storage. */
    private int rows;
    /** Amount of columns in a matrix storage. */
    private int cols;

    /**
     *
     */
    public ExampleMatrixStorage() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in a matrix storage.
     * @param cols Amount of columns in a matrix storage.
     */
    ExampleMatrixStorage(int rows, int cols) {
        assert rows > 0;
        assert cols > 0;

        this.data = new double[rows][cols];
        this.rows = rows;
        this.cols = cols;
    }

    /**
     * @param data Backing data array.
     */
    ExampleMatrixStorage(double[][] data) {
        assert data != null;
        assert data[0] != null;

        this.data = data;
        this.rows = data.length;
        this.cols = data[0].length;

        assert rows > 0;
        assert cols > 0;
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
    @Override public int storageMode() {
        // This value indicate that we store matrix data by rows.
        return StorageConstants.ROW_STORAGE_MODE;
    }

    /** {@inheritDoc} */
    @Override public int accessMode() {
        return StorageConstants.RANDOM_ACCESS_MODE;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return MatrixUtil.flatten(data, StorageConstants.ROW_STORAGE_MODE);
    }

    /** {@inheritDoc */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);

        out.writeObject(data);
    }

    /** {@inheritDoc */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();

        data = (double[][])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res += res * 37 + rows;
        res += res * 37 + cols;
        res += res * 37 + Arrays.deepHashCode(data);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ExampleMatrixStorage that = (ExampleMatrixStorage)o;

        return Arrays.deepEquals(data, that.data);
    }
}

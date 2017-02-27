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

package org.apache.ignite.math.impls;

import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.*;
import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * TODO: add description.
 */
public abstract class AbstractMatrix implements Matrix {
    // Stochastic sparsity analysis.
    private final double Z95 = 1.959964;
    private final double Z80 = 1.281552;
    private final int MAX_SAMPLES = 500;
    private final int MIN_SAMPLES = 15;

    // Matrix storage implementation.
    private MatrixStorage sto;

    // Meta attribute storage.
    private Map<String, Object> meta = new HashMap<>();

    // Matrix's GUID.
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /**
     *
     * @param sto
     */
    public AbstractMatrix(MatrixStorage sto) {
        this.sto = sto;
    }

    /**
     *
     */
    public AbstractMatrix() {
        sto = new MatrixNullStorage();
    }

    /**
     *
     * @param sto
     */
    protected void setStorage(MatrixStorage sto) {
        this.sto = sto == null ? new MatrixNullStorage() : sto;
    }

    /**
     *
     * @param row
     * @param col
     * @param v
     */
    protected void storageSet(int row, int col, double v) {
        sto.set(row, col, v);
    }

    /**
     *
     * @param row
     * @param col
     * @return
     */
    protected double storageGet(int row, int col) {
        return sto.get(row, col);
    }

    @Override
    public Matrix swapRows(int row1, int row2) {
        checkRowIndex(row1);
        checkRowIndex(row2);

        int cols = columnSize();

        for (int y = 0; y < cols; y++) {
            double v = getX(row1, y);
            
            setX(row1, y, getX(row2, y));
            setX(row2, y, v);
        }

        return this;
    }

    @Override
    public Matrix swapColumns(int col1, int col2) {
        checkColumnIndex(col1);
        checkColumnIndex(col2);

        int rows = rowSize();

        for (int x = 0; x < rows; x++) {
            double v = getX(x, col1);

            setX(x, col1, getX(x, col2));
            setX(x, col2, v);
        }

        return this;
    }

    @Override
    public MatrixStorage getStorage() {
        return sto;
    }

    @Override
    public boolean isSequentialAccess() {
        return sto.isSequentialAccess();
    }

    @Override
    public boolean isDense() {
        return sto.isDense();
    }

    @Override
    public double getLookupCost() {
        return sto.getLookupCost();
    }

    @Override
    public boolean isAddConstantTime() {
        return sto.isAddConstantTime();
    }

    @Override
    public boolean isArrayBased() {
        return sto.isArrayBased();
    }

    /**
     * Check row index bounds.
     *
     * @param row Row index.
     */
    private void checkRowIndex(int row) {
        if (row < 0 || row >= sto.rowSize())
            throw new RowIndexException(row);
    }

    /**
     * Check column index bounds.
     *
     * @param col Column index.
     */
    private void checkColumnIndex(int col) {
        if (col < 0 || col >= sto.columnSize())
            throw new ColumnIndexException(col);
    }

    /**
     * Check column and row index bounds.
     *
     * @param row Row index.
     * @param col Column index.
     */
    private void checkIndex(int row, int col) {
        checkRowIndex(row);
        checkColumnIndex(col);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeObject(meta);
        out.writeObject(guid);
    }

    @Override
    public Map<String, Object> getMetaStorage() {
        return meta;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (MatrixStorage)in.readObject();
        meta = (Map<String, Object>)in.readObject();
        guid = (IgniteUuid)in.readObject();
    }

    @Override
    public Matrix assign(double val) {
        if (sto.isArrayBased())
            for (double[] column : sto.data())
                Arrays.fill(column, val);
        else {
            int rows = sto.rowSize();
            int cols = sto.columnSize();

            for (int x = 0; x < rows; x++)
                for (int y = 0; y < cols; y++)
                    storageSet(x, y, val);
        }

        return this;
    }

    private void checkCardinality(Matrix mtx) {
        checkCardinality(mtx.rowSize(), mtx.columnSize());
    }

    private void checkCardinality(int rows, int cols) {
        if (rows != sto.rowSize())
            throw new CardinalityException(rowSize(), rows);

        if (cols != sto.columnSize())
            throw new CardinalityException(columnSize(), cols);
    }

    @Override
    public Matrix assign(double[][] vals) {
        checkCardinality(vals.length, vals[0].length);

        int rows = sto.rowSize();
        int cols = sto.columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, vals[x][y]);

        return this;
    }

    @Override
    public Matrix assign(Matrix mtx) {
        checkCardinality(mtx);

        int rows = sto.rowSize();
        int cols = sto.columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, mtx.getX(x, y));

        return this;
    }

    @Override
    public Matrix map(DoubleFunction<Double> fun) {
        int rows = sto.rowSize();
        int cols = sto.columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, fun.apply(storageGet(x, y)));

        return this;
    }

    @Override
    public Matrix map(Matrix mtx, BiFunction<Double, Double, Double> fun) {
        checkCardinality(mtx);

        int rows = sto.rowSize();
        int cols = sto.columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, fun.apply(storageGet(x, y), mtx.getX(x, y)));

        return this;
    }

    @Override
    public Matrix assignColumn(int col, Vector vec) {
        checkColumnIndex(col);

        int rows = sto.rowSize();

        for (int x = 0; x < rows; x++)
            storageSet(x, col, vec.getX(x));

        return this;
    }

    @Override
    public Matrix assignRow(int row, Vector vec) {
        checkRowIndex(row);

        int cols = sto.columnSize();

        if (cols != vec.size())
            throw new CardinalityException(cols, vec.size());

        if (sto.isArrayBased() && vec.getStorage().isArrayBased())
            System.arraycopy(vec.getStorage().data(), 0, sto.data()[row], 0, cols);
        else
            for (int y = 0; y < cols; y++)
                storageSet(row, y, vec.getX(y));

        return this;
    }

    @Override
    public Vector foldRows(Function<Vector, Double> fun) {
        int rows = rowSize();

        Vector vec = likeVector(rows);

        for (int i = 0; i < rows; i++)
            vec.setX(i, fun.apply(viewRow(i)));

        return vec;
    }

    @Override
    public Vector foldColumns(Function<Vector, Double> fun) {
        int cols = columnSize();

        Vector vec = likeVector(cols);

        for (int i = 0; i < cols; i++)
            vec.setX(i, fun.apply(viewColumn(i)));

        return vec;
    }

    @Override
    public <T> T foldMap(BiFunction<T, Double, T> foldFun, DoubleFunction<Double> mapFun, T zeroVal) {
        T res = zeroVal;

        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                res = foldFun.apply(res, mapFun.apply(storageGet(x, y)));

        return res;
    }

    @Override
    public int columnSize() {
        return sto.columnSize();
    }

    @Override
    public int rowSize() {
        return sto.rowSize();
    }

    @Override
    public double determinant() {
        int rows = rowSize();
        int cols = columnSize();

        if (rows != cols)
            throw new CardinalityException(rows, cols);

        if (rows == 2)
            return getX(0, 0) * getX(1, 1) - getX(0, 1) * getX(1, 0);
        else {
            int sign = 1;
            double ret = 0.0;

            for (int i = 0; i < cols; i++) {
                Matrix minor = like(rows - 1, cols - 1);

                for (int j = 1; j < rows; j++) {
                    boolean flag = false;

                    for (int k = 0; k < cols; k++) {
                        if (k == i) {
                            flag = true;
                            continue;
                        }

                        minor.set(j - 1, flag ? k - 1 : k, getX(j, k));
                    }
                }

                ret += getX(0, i) * sign * minor.determinant();
                sign *= -1;

            }

            return ret;
        }
    }

    @Override
    public Matrix divide(double d) {
        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                setX(x, y, getX(x, y) / d);

        return this;
    }

    @Override
    public double get(int row, int col) {
        checkIndex(row, col);

        return storageGet(row, col);
    }

    @Override
    public double getX(int row, int col) {
        return storageGet(row, col);
    }

    @Override
    public Matrix minus(Matrix mtx) {
        int rows = rowSize();
        int cols = columnSize();

        checkCardinality(rows, cols);

        Matrix res = like(rows, cols);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                res.setX(x, y, getX(x, y) - mtx.getX(x, y));

        return res;
    }

    @Override
    public Matrix plus(double x) {
        Matrix copy = copy();

        copy.map(Functions.plus(x));

        return copy;
    }

    @Override
    public Matrix plus(Matrix mtx) {
        int rows = rowSize();
        int cols = columnSize();

        checkCardinality(rows, cols);

        Matrix res = like(rows, cols);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                res.setX(x, y, getX(x, y) + mtx.getX(x, y));

        return res;

    }

    @Override
    public IgniteUuid guid() {
        return guid;
    }

    @Override
    public Matrix set(int row, int col, double val) {
        checkIndex(row, col);

        storageSet(row, col, val);

        return this;
    }

    @Override
    public Matrix setRow(int row, double[] data) {
        checkRowIndex(row);

        int cols = columnSize();

        if (cols != data.length)
            throw new CardinalityException(cols, data.length);

        if (sto.isArrayBased())
            System.arraycopy(data, 0, sto.data()[row], 0, cols);
        else
            for (int y = 0; y < cols; y++)
                setX(row, y, data[y]);

        return this;
    }

    @Override
    public Matrix setColumn(int col, double[] data) {
        checkColumnIndex(col);

        int rows = rowSize();

        if (rows != data.length)
            throw new CardinalityException(rows, data.length);

        for (int x = 0; x < rows; x++)
            setX(x, col, data[x]);

        return this;
    }

    @Override
    public Matrix setX(int row, int col, double val) {
        storageSet(row, col, val);

        return this;
    }

    @Override
    public Matrix times(double x) {
        Matrix copy = copy();

        copy.map(Functions.mult(x));

        return copy;
    }

    @Override
    public double maxAbsRowSumNorm() {
        double max = 0.0;

        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++) {
            int sum = 0;

            for (int y = 0; y < cols; y++)
                sum += (int)Math.abs(getX(x, y));

            if (sum > max)
                max = sum;
        }
        
        return max;
    }

    @Override
    public Vector times(Vector vec) {
        int cols = columnSize();

        if (cols != vec.size())
            throw new CardinalityException(cols, vec.size());

        int rows = rowSize();

        Vector res = likeVector(rows);

        for (int x = 0; x < rows; x++)
            res.setX(x, vec.dot(viewRow(x)));

        return res;
    }

    @Override
    public Matrix times(Matrix mtx) {
        int cols = columnSize();

        if (cols != mtx.rowSize())
            throw new CardinalityException(cols, mtx.rowSize());

        int rows = rowSize();

        int mtxCols = mtx.columnSize();

        Matrix res = like(rows, mtxCols);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < mtxCols; y++) {
                double sum = 0.0;

                for (int k = 0; k < cols; k++) {
                    sum += getX(x, k) * mtx.getX(k, y);
                }

                res.setX(x, y, sum);
            }

        return res;
    }

    @Override
    public double sum() {
        int rows = rowSize();
        int cols = columnSize();

        double sum = 0.0;

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                sum += getX(x, y);

        return sum;
    }

    @Override
    public Matrix transpose() {
        int rows = rowSize();
        int cols = columnSize();

        Matrix mtx = like(cols, rows);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                mtx.setX(y, x, getX(x, y));

        return mtx;
    }

    @Override
    public boolean density(double threshold) {
        assert threshold >= 0.0 && threshold <= 1.0;

        int n = MIN_SAMPLES;
        int rows = rowSize();
        int cols = columnSize();

        double mean = 0.0;
        double pq = threshold * (1 - threshold);

        Random rnd = new Random();

        for (int i = 0; i < MIN_SAMPLES; i++)
            if (getX(rnd.nextInt(rows), rnd.nextInt(cols)) != 0.0)
                mean++;

        mean /= MIN_SAMPLES;
        
        double iv = Z80 * Math.sqrt(pq / n);

        if (mean < threshold - iv)
            return false; // Sparse.
        else if (mean > threshold + iv)
            return true; // Dense.

        while (n < MAX_SAMPLES) {
            // Determine upper bound we may need for 'n' to likely relinquish the uncertainty.
            // Here, we use confidence interval formula but solved for 'n'.
            double ivX = Math.max(Math.abs(threshold - mean), 1e-11);

            double stdErr = ivX / Z80;
            double nX = Math.min(Math.max((int)Math.ceil(pq / (stdErr * stdErr)), n), MAX_SAMPLES) - n;

            double meanNext = 0.0;

            for (int i = 0; i < nX; i++)
                if (getX(rnd.nextInt(rows), rnd.nextInt(cols)) != 0.0) meanNext++;

            mean = (n * mean + meanNext) / (n + nX);
            
            n += nX;

            // Are we good now?
            iv = Z80 * Math.sqrt(pq / n);

            if (mean < threshold - iv)
                return false; // Sparse.
            else if (mean > threshold + iv)
                return true; // Dense.
        }

        return mean > threshold; // Dense if mean > threshold.
    }

    @Override
    public Matrix viewPart(int[] offset, int[] size) {
        return new MatrixView(this, offset[0], offset[1], size[0], size[1]);
    }

    @Override
    public Matrix viewPart(int rowOff, int rows, int colOff, int cols) {
        return viewPart(new int[] { rowOff, colOff}, new int[] {rows, cols});
    }

    @Override
    public Vector viewRow(int row) {
        return new MatrixVectorView(this, row, 0, 1, 0);
    }

    @Override
    public Vector viewColumn(int col) {
        return new MatrixVectorView(this, 0, col, 0, 1);
    }

    @Override
    public Vector viewDiagonal() {
        return new MatrixVectorView(this, 0, 0, 1, 1);
    }
}

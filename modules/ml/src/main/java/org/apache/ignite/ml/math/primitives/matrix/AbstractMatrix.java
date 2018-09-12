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

package org.apache.ignite.ml.math.primitives.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.Blas;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.ColumnIndexException;
import org.apache.ignite.ml.math.exceptions.RowIndexException;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.functions.IntIntToDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.apache.ignite.ml.math.util.MatrixUtil;

/**
 * This class provides a helper implementation of the {@link Matrix}
 * interface to minimize the effort required to implement it.
 * Subclasses may override some of the implemented methods if a more
 * specific or optimized implementation is desirable.
 */
public abstract class AbstractMatrix implements Matrix {
    // Stochastic sparsity analysis.
    /** */
    private static final double Z95 = 1.959964;
    /** */
    private static final double Z80 = 1.281552;
    /** */
    private static final int MAX_SAMPLES = 500;
    /** */
    private static final int MIN_SAMPLES = 15;

    /** Cached minimum element. */
    private Element minElm;
    /** Cached maximum element. */
    private Element maxElm = null;

    /** Matrix storage implementation. */
    private MatrixStorage sto;

    /** Meta attributes storage. */
    private Map<String, Object> meta = new HashMap<>();

    /** Matrix's GUID. */
    private IgniteUuid guid = IgniteUuid.randomUuid();

    /**
     * @param sto Backing {@link MatrixStorage}.
     */
    public AbstractMatrix(MatrixStorage sto) {
        this.sto = sto;
    }

    /**
     *
     */
    public AbstractMatrix() {
        // No-op.
    }

    /**
     * @param sto Backing {@link MatrixStorage}.
     */
    protected void setStorage(MatrixStorage sto) {
        assert sto != null;

        this.sto = sto;
    }

    /**
     * @param row Row index in the matrix.
     * @param col Column index in the matrix.
     * @param v Value to set.
     */
    protected void storageSet(int row, int col, double v) {
        sto.set(row, col, v);

        // Reset cached values.
        minElm = maxElm = null;
    }

    /**
     * @param row Row index in the matrix.
     * @param col Column index in the matrix.
     */
    protected double storageGet(int row, int col) {
        return sto.get(row, col);
    }

    /** {@inheritDoc} */
    @Override public Element maxElement() {
        if (maxElm == null) {
            double max = Double.NEGATIVE_INFINITY;
            int row = 0, col = 0;

            int rows = rowSize();
            int cols = columnSize();

            for (int x = 0; x < rows; x++)
                for (int y = 0; y < cols; y++) {
                    double d = storageGet(x, y);

                    if (d > max) {
                        max = d;
                        row = x;
                        col = y;
                    }
                }

            maxElm = mkElement(row, col);
        }

        return maxElm;
    }

    /** {@inheritDoc} */
    @Override public Element minElement() {
        if (minElm == null) {
            double min = Double.MAX_VALUE;
            int row = 0, col = 0;

            int rows = rowSize();
            int cols = columnSize();

            for (int x = 0; x < rows; x++)
                for (int y = 0; y < cols; y++) {
                    double d = storageGet(x, y);

                    if (d < min) {
                        min = d;
                        row = x;
                        col = y;
                    }
                }

            minElm = mkElement(row, col);
        }

        return minElm;
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        return maxElement().get();
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        return minElement().get();
    }

    /**
     * @param row Row index in the matrix.
     * @param col Column index in the matrix.
     */
    private Element mkElement(int row, int col) {
        return new Element() {
            /** {@inheritDoc} */
            @Override public double get() {
                return storageGet(row, col);
            }

            /** {@inheritDoc} */
            @Override public int row() {
                return row;
            }

            /** {@inheritDoc} */
            @Override public int column() {
                return col;
            }

            /** {@inheritDoc} */
            @Override public void set(double d) {
                storageSet(row, col, d);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Element getElement(int row, int col) {
        return mkElement(row, col);
    }

    /** {@inheritDoc} */
    @Override public Matrix swapRows(int row1, int row2) {
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

    /** {@inheritDoc} */
    @Override public Matrix swapColumns(int col1, int col2) {
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

    /** {@inheritDoc} */
    @Override public MatrixStorage getStorage() {
        return sto;
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return sto.isSequentialAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return sto.isDense();
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return sto.isRandomAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return sto.isDistributed();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return sto.isArrayBased();
    }

    /**
     * Check row index bounds.
     *
     * @param row Row index.
     */
    private void checkRowIndex(int row) {
        if (row < 0 || row >= rowSize())
            throw new RowIndexException(row);
    }

    /**
     * Check column index bounds.
     *
     * @param col Column index.
     */
    private void checkColumnIndex(int col) {
        if (col < 0 || col >= columnSize())
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeObject(meta);
        out.writeObject(guid);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getMetaStorage() {
        return meta;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (MatrixStorage)in.readObject();
        meta = (Map<String, Object>)in.readObject();
        guid = (IgniteUuid)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        if (sto.isArrayBased())
            Arrays.fill(sto.data(), val);
        else {
            int rows = rowSize();
            int cols = columnSize();

            for (int x = 0; x < rows; x++)
                for (int y = 0; y < cols; y++)
                    storageSet(x, y, val);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(IntIntToDoubleFunction fun) {
        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, fun.apply(x, y));

        return this;
    }

    /** */
    private void checkCardinality(Matrix mtx) {
        checkCardinality(mtx.rowSize(), mtx.columnSize());
    }

    /** */
    private void checkCardinality(int rows, int cols) {
        if (rows != rowSize())
            throw new CardinalityException(rowSize(), rows);

        if (cols != columnSize())
            throw new CardinalityException(columnSize(), cols);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double[][] vals) {
        checkCardinality(vals.length, vals[0].length);

        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, vals[x][y]);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(Matrix mtx) {
        checkCardinality(mtx);

        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, mtx.getX(x, y));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, fun.apply(storageGet(x, y)));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix map(Matrix mtx, IgniteBiFunction<Double, Double, Double> fun) {
        checkCardinality(mtx);

        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                storageSet(x, y, fun.apply(storageGet(x, y), mtx.getX(x, y)));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> allSpliterator() {
        return new Spliterator<Double>() {
            /** {@inheritDoc} */
            @Override public boolean tryAdvance(Consumer<? super Double> act) {
                int rLen = rowSize();
                int cLen = columnSize();

                for (int i = 0; i < rLen; i++)
                    for (int j = 0; j < cLen; j++)
                        act.accept(storageGet(i, j));

                return true;
            }

            /** {@inheritDoc} */
            @Override public Spliterator<Double> trySplit() {
                return null; // No Splitting.
            }

            /** {@inheritDoc} */
            @Override public long estimateSize() {
                return rowSize() * columnSize();
            }

            /** {@inheritDoc} */
            @Override public int characteristics() {
                return ORDERED | SIZED;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int nonZeroElements() {
        int cnt = 0;

        for (int i = 0; i < rowSize(); i++)
            for (int j = 0; j < rowSize(); j++)
                if (get(i, j) != 0.0)
                    cnt++;

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> nonZeroSpliterator() {
        return new Spliterator<Double>() {
            /** {@inheritDoc} */
            @Override public boolean tryAdvance(Consumer<? super Double> act) {
                int rLen = rowSize();
                int cLen = columnSize();

                for (int i = 0; i < rLen; i++)
                    for (int j = 0; j < cLen; j++) {
                        double val = storageGet(i, j);

                        if (val != 0.0)
                            act.accept(val);
                    }
                return true;
            }

            /** {@inheritDoc} */
            @Override public Spliterator<Double> trySplit() {
                return null; // No Splitting.
            }

            /** {@inheritDoc} */
            @Override public long estimateSize() {
                return nonZeroElements();
            }

            /** {@inheritDoc} */
            @Override public int characteristics() {
                return ORDERED | SIZED;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Matrix assignColumn(int col, Vector vec) {
        checkColumnIndex(col);

        int rows = rowSize();

        for (int x = 0; x < rows; x++)
            storageSet(x, col, vec.getX(x));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix assignRow(int row, Vector vec) {
        checkRowIndex(row);

        int cols = columnSize();

        if (cols != vec.size())
            throw new CardinalityException(cols, vec.size());

        // TODO: IGNITE-5777, use Blas for this.
        for (int y = 0; y < cols; y++)
            storageSet(row, y, vec.getX(y));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Vector foldRows(IgniteFunction<Vector, Double> fun) {
        int rows = rowSize();

        Vector vec = likeVector(rows);

        for (int i = 0; i < rows; i++)
            vec.setX(i, fun.apply(viewRow(i)));

        return vec;
    }

    /** {@inheritDoc} */
    @Override public Vector foldColumns(IgniteFunction<Vector, Double> fun) {
        int cols = columnSize();

        Vector vec = likeVector(cols);

        for (int i = 0; i < cols; i++)
            vec.setX(i, fun.apply(viewColumn(i)));

        return vec;
    }

    /** {@inheritDoc} */
    @Override public <T> T foldMap(IgniteBiFunction<T, Double, T> foldFun, IgniteDoubleFunction<Double> mapFun,
        T zeroVal) {
        T res = zeroVal;

        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                res = foldFun.apply(res, mapFun.apply(storageGet(x, y)));

        return res;
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return sto.columnSize();
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return sto.rowSize();
    }

    /** {@inheritDoc} */
    @Override public Matrix divide(double d) {
        int rows = rowSize();
        int cols = columnSize();

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                setX(x, y, getX(x, y) / d);

        return this;
    }

    /** {@inheritDoc} */
    @Override public double get(int row, int col) {
        checkIndex(row, col);

        return storageGet(row, col);
    }

    /** {@inheritDoc} */
    @Override public double getX(int row, int col) {
        return storageGet(row, col);
    }

    /** {@inheritDoc} */
    @Override public Matrix minus(Matrix mtx) {
        int rows = rowSize();
        int cols = columnSize();

        checkCardinality(rows, cols);

        Matrix res = like(rows, cols);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                res.setX(x, y, getX(x, y) - mtx.getX(x, y));

        return res;
    }

    /** {@inheritDoc} */
    @Override public Matrix plus(double x) {
        Matrix cp = copy();

        cp.map(Functions.plus(x));

        return cp;
    }

    /** {@inheritDoc} */
    @Override public Matrix plus(Matrix mtx) {
        int rows = rowSize();
        int cols = columnSize();

        checkCardinality(rows, cols);

        Matrix res = like(rows, cols);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                res.setX(x, y, getX(x, y) + mtx.getX(x, y));

        return res;

    }

    /** {@inheritDoc} */
    @Override public IgniteUuid guid() {
        return guid;
    }

    /** {@inheritDoc} */
    @Override public Matrix set(int row, int col, double val) {
        checkIndex(row, col);

        storageSet(row, col, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix setRow(int row, double[] data) {
        checkRowIndex(row);

        int cols = columnSize();

        if (cols != data.length)
            throw new CardinalityException(cols, data.length);
        // TODO: IGNITE-5777, use Blas for this.
        for (int y = 0; y < cols; y++)
            setX(row, y, data[y]);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Vector getRow(int row) {
        checkRowIndex(row);

        Vector res = new DenseVector(columnSize());

        for (int i = 0; i < columnSize(); i++)
            res.setX(i, getX(row, i));

        return res;
    }

    /** {@inheritDoc} */
    @Override public Matrix setColumn(int col, double[] data) {
        checkColumnIndex(col);

        int rows = rowSize();

        if (rows != data.length)
            throw new CardinalityException(rows, data.length);

        for (int x = 0; x < rows; x++)
            setX(x, col, data[x]);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Vector getCol(int col) {
        checkColumnIndex(col);

        Vector res;

        if (isDistributed())
            res = MatrixUtil.likeVector(this, rowSize());
        else
            res = new DenseVector(rowSize());

        for (int i = 0; i < rowSize(); i++)
            res.setX(i, getX(i, col));

        return res;
    }

    /** {@inheritDoc} */
    @Override public Matrix setX(int row, int col, double val) {
        storageSet(row, col, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix times(double x) {
        Matrix cp = copy();

        cp.map(Functions.mult(x));

        return cp;
    }

    /** {@inheritDoc} */
    @Override public Vector times(Vector vec) {
        int cols = columnSize();

        if (cols != vec.size())
            throw new CardinalityException(cols, vec.size());

        int rows = rowSize();

        Vector res = likeVector(rows);

        Blas.gemv(1, this, vec, 0, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Matrix times(Matrix mtx) {
        int cols = columnSize();

        if (cols != mtx.rowSize())
            throw new CardinalityException(cols, mtx.rowSize());

        Matrix res = like(rowSize(), mtx.columnSize());

        Blas.gemm(1, this, mtx, 0, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        int rows = rowSize();
        int cols = columnSize();

        double sum = 0.0;

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                sum += getX(x, y);

        return sum;
    }

    /** {@inheritDoc} */
    @Override public Matrix transpose() {
        int rows = rowSize();
        int cols = columnSize();

        Matrix mtx = like(cols, rows);

        for (int x = 0; x < rows; x++)
            for (int y = 0; y < cols; y++)
                mtx.setX(y, x, getX(x, y));

        return mtx;
    }

    /** {@inheritDoc} */
    @Override public boolean density(double threshold) {
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

            if (nX < 1.0) // IMPL NOTE this can happen with threshold 1.0
                nX = 1.0;

            double meanNext = 0.0;

            for (int i = 0; i < nX; i++)
                if (getX(rnd.nextInt(rows), rnd.nextInt(cols)) != 0.0)
                    meanNext++;

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

    /** {@inheritDoc} */
    @Override public Vector viewRow(int row) {
        return new VectorizedViewMatrix(this, row, 0, 0, 1);
    }

    /** {@inheritDoc} */
    @Override public Vector viewColumn(int col) {
        return new VectorizedViewMatrix(this, 0, col, 1, 0);
    }

    /** {@inheritDoc} */
    @Override public Vector viewDiagonal() {
        return new VectorizedViewMatrix(this, 0, 0, 1, 1);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        getStorage().destroy();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        Matrix cp = like(rowSize(), columnSize());

        cp.assign(this);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + guid.hashCode();
        res = res * 37 + sto.hashCode();
        res = res * 37 + meta.hashCode();

        return res;
    }

    /**
     * {@inheritDoc}
     *
     * We ignore guid's for comparisons.
     */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        AbstractMatrix that = (AbstractMatrix)o;

        MatrixStorage sto = getStorage();

        return (sto != null ? sto.equals(that.getStorage()) : that.getStorage() == null);
    }

    /** {@inheritDoc} */
    @Override public void compute(int row, int col, IgniteTriFunction<Integer, Integer, Double, Double> f) {
        setX(row, col, f.apply(row, col, getX(row, col)));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Matrix [rows=" + rowSize() + ", cols=" + columnSize() + "]";
    }
}

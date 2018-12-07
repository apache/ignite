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

package org.apache.ignite.ml.nn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Spliterator;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.functions.IntIntToDoubleFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.MatrixStorage;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Convenient way to create matrix of replicated columns or rows from vector.
 * This class should be considered as utility class: not all matrix methods are implemented here, only those which
 * were necessary for MLPs.
 */
class ReplicatedVectorMatrix implements Matrix {
    /**
     * Vector to replicate.
     */
    private Vector vector;

    /**
     * Flag determining is vector replicated as column or row.
     */
    private boolean asCol;

    /**
     * Count of vector replications.
     */
    private int replicationCnt;

    /**
     * Construct ReplicatedVectorMatrix.
     *
     * @param vector Vector to replicate.
     * @param replicationCnt Count of replications.
     * @param asCol Should vector be replicated as a column or as a row.
     */
    ReplicatedVectorMatrix(Vector vector, int replicationCnt, boolean asCol) {
        this.vector = vector;
        this.asCol = asCol;
        this.replicationCnt = replicationCnt;
    }

    /**
     * Constructor for externalization.
     */
    public ReplicatedVectorMatrix() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return vector.isSequentialAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return vector.isRandomAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return vector.isDense();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return vector.isArrayBased();
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return vector.isDistributed();
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        return vector.maxValue();
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        return vector.minValue();
    }

    /** {@inheritDoc} */
    @Override public Element maxElement() {
        return new Element() {
            @Override public double get() {
                return vector.maxElement().get();
            }

            @Override public int row() {
                return asCol ? vector.maxElement().index() : 0;
            }

            @Override public int column() {
                return asCol ? 0 : vector.maxElement().index();
            }

            @Override public void set(double val) {

            }
        };
    }

    /** {@inheritDoc} */
    @Override public Element minElement() {
        return new Element() {
            @Override public double get() {
                return vector.minElement().get();
            }

            @Override public int row() {
                return asCol ? vector.minElement().index() : 0;
            }

            @Override public int column() {
                return asCol ? 0 : vector.minElement().index();
            }

            @Override public void set(double val) {

            }
        };
    }

    /** {@inheritDoc} */
    @Override public Element getElement(int row, int col) {
        Vector.Element el = asCol ? vector.getElement(row) : vector.getElement(col);
        int r = asCol ? el.index() : 0;
        int c = asCol ? 0 : el.index();

        return new Element() {
            @Override public double get() {
                return el.get();
            }

            @Override public int row() {
                return r;
            }

            @Override public int column() {
                return c;
            }

            @Override public void set(double val) {

            }
        };
    }

    /** {@inheritDoc} */
    @Override public Matrix swapRows(int row1, int row2) {
        return asCol ? new ReplicatedVectorMatrix(swap(row1, row2), replicationCnt, asCol) : this;
    }

    /** {@inheritDoc} */
    private Vector swap(int idx1, int idx2) {
        double val = vector.getX(idx1);

        vector.setX(idx1, vector.getX(idx2));
        vector.setX(idx2, val);

        return vector;
    }

    /** {@inheritDoc} */
    @Override public Matrix swapColumns(int col1, int col2) {
        return asCol ? this : new ReplicatedVectorMatrix(swap(col1, col2), replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        return new ReplicatedVectorMatrix(vector.assign(val), replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double[][] vals) {
        return new DenseMatrix(vals);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(Matrix mtx) {
        return mtx.copy();
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(IntIntToDoubleFunction fun) {
        Vector vec = asCol ? this.vector.assign(idx -> fun.apply(idx, 0)) : this.vector.assign(idx -> fun.apply(0, idx));
        return new ReplicatedVectorMatrix(vec, replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        Vector vec = vector.map(fun);
        return new ReplicatedVectorMatrix(vec, replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(Matrix mtx, IgniteBiFunction<Double, Double, Double> fun) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int nonZeroElements() {
        return vector.nonZeroElements() * (asCol ? columnSize() : rowSize());
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> allSpliterator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Double> nonZeroSpliterator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix assignColumn(int col, Vector vec) {
        int rows = asCol ? vector.size() : replicationCnt;
        int cols = asCol ? replicationCnt : vector.size();
        int times = asCol ? cols : rows;

        Matrix res = new DenseMatrix(rows, cols);

        IgniteBiConsumer<Integer, Vector> replicantAssigner = asCol ? res::assignColumn : res::assignRow;
        IgniteBiConsumer<Integer, Vector> assigner = res::assignColumn;

        assign(replicantAssigner, assigner, vector, vec, times, col);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Matrix assignRow(int row, Vector vec) {
        int rows = asCol ? vector.size() : replicationCnt;
        int cols = asCol ? replicationCnt : vector.size();
        int times = asCol ? cols : rows;

        Matrix res = new DenseMatrix(rows, cols);

        IgniteBiConsumer<Integer, Vector> replicantAssigner = asCol ? res::assignColumn : res::assignRow;
        IgniteBiConsumer<Integer, Vector> assigner = res::assignRow;

        assign(replicantAssigner, assigner, vector, vec, times, row);

        return res;
    }

    /** */
    private void assign(IgniteBiConsumer<Integer, Vector> replicantAssigner,
        IgniteBiConsumer<Integer, Vector> assigner, Vector replicant, Vector vector, int times, int idx) {
        for (int i = 0; i < times; i++)
            replicantAssigner.accept(i, replicant);
        assigner.accept(idx, vector);
    }

    /** {@inheritDoc} */
    @Override public Vector foldRows(IgniteFunction<Vector, Double> fun) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector foldColumns(IgniteFunction<Vector, Double> fun) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T foldMap(IgniteBiFunction<T, Double, T> foldFun, IgniteDoubleFunction<Double> mapFun,
        T zeroVal) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean density(double threshold) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return asCol ? replicationCnt : vector.size();
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return asCol ? vector.size() : replicationCnt;
    }

    /** {@inheritDoc} */
    @Override public Matrix divide(double x) {
        return new ReplicatedVectorMatrix(vector.divide(x), replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public double get(int row, int col) {
        return asCol ? vector.get(row) : vector.get(col);
    }

    /** {@inheritDoc} */
    @Override public double getX(int row, int col) {
        return asCol ? vector.getX(row) : vector.getX(col);
    }

    /** {@inheritDoc} */
    @Override public MatrixStorage getStorage() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        Vector cp = vector.copy();
        return new ReplicatedVectorMatrix(cp, replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        Vector lk = vector.like(vector.size());
        return new ReplicatedVectorMatrix(lk, replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix minus(Matrix mtx) {
        throw new UnsupportedOperationException();
    }

    /**
     * Specialized optimized version of minus for ReplicatedVectorMatrix.
     *
     * @param mtx Matrix to be subtracted.
     * @return new ReplicatedVectorMatrix resulting from subtraction.
     */
    public Matrix minus(ReplicatedVectorMatrix mtx) {
        if (isColumnReplicated() == mtx.isColumnReplicated()) {
            checkCardinality(mtx.rowSize(), mtx.columnSize());

            Vector minus = vector.minus(mtx.replicant());

            return new ReplicatedVectorMatrix(minus, replicationCnt, asCol);
        }

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix plus(double x) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix plus(Matrix mtx) {
        throw new UnsupportedOperationException();
    }

    /**
     * Specialized optimized version of plus for ReplicatedVectorMatrix.
     *
     * @param mtx Matrix to be added.
     * @return new ReplicatedVectorMatrix resulting from addition.
     */
    public Matrix plus(ReplicatedVectorMatrix mtx) {
        if (isColumnReplicated() == mtx.isColumnReplicated()) {
            checkCardinality(mtx.rowSize(), mtx.columnSize());

            Vector plus = vector.plus(mtx.replicant());

            return new ReplicatedVectorMatrix(plus, replicationCnt, asCol);
        }

        throw new UnsupportedOperationException();
    }

    /**
     * Checks that dimensions of this matrix are equal to given dimensions.
     *
     * @param rows Rows.
     * @param cols Columns.
     */
    private void checkCardinality(int rows, int cols) {
        if (rows != rowSize())
            throw new CardinalityException(rowSize(), rows);

        if (cols != columnSize())
            throw new CardinalityException(columnSize(), cols);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid guid() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Matrix set(int row, int col, double val) {
        vector.set(asCol ? row : col, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix setRow(int row, double[] data) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Vector getRow(int row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Matrix setColumn(int col, double[] data) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Vector getCol(int col) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Matrix setX(int row, int col, double val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Matrix times(double x) {
        return new ReplicatedVectorMatrix(vector.times(x), replicationCnt, asCol);
    }

    /** {@inheritDoc} */
    @Override public Matrix times(Matrix mtx) {
        if (!asCol) {
            Vector row = vector.like(mtx.columnSize());

            for (int i = 0; i < mtx.columnSize(); i++)
                row.setX(i, vector.dot(mtx.getCol(i)));

            return new ReplicatedVectorMatrix(row, replicationCnt, false);

        }
        else
            throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector times(Vector vec) {
        Vector res = vec.like(vec.size());
        if (asCol) {
            for (int i = 0; i < rowSize(); i++)
                res.setX(i, vec.sum() * vector.getX(i));
        }
        else {
            double val = vector.dot(vec);

            for (int i = 0; i < rowSize(); i++)
                res.setX(i, val);
        }
        return res;
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return vector.sum() * replicationCnt;
    }

    /** {@inheritDoc} */
    @Override public Matrix transpose() {
        return new ReplicatedVectorMatrix(vector, replicationCnt, !asCol);
    }

    /** {@inheritDoc} */
    @Override public Vector viewRow(int row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Vector viewColumn(int col) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Vector viewDiagonal() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void compute(int row, int col, IgniteTriFunction<Integer, Integer, Double, Double> f) {
        // This operation cannot be performed because computing function depends on both indexes and therefore
        // result of compute will be in general case not ReplicatedVectorMatrix.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getMetaStorage() {
        return null;
    }

    /**
     * Returns true if matrix constructed by replicating vector as column and false otherwise.
     */
    public boolean isColumnReplicated() {
        return asCol;
    }

    /**
     * Returns replicated vector.
     */
    public Vector replicant() {
        return vector;
    }
}

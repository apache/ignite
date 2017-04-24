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

package org.apache.ignite.ml.math.impls.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.IndexException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.storage.matrix.PivotedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.PivotedVectorView;

/**
 * Pivoted (index mapped) view over another matrix implementation.
 */
public class PivotedMatrixView extends AbstractMatrix {
    /** Pivoted matrix. */
    private Matrix mtx;

    /**
     *
     */
    public PivotedMatrixView() {
        // No-op.
    }

    /**
     * @param mtx Parent matrix.
     * @param rowPivot Pivot array for rows.
     * @param colPivot Pivot array for columns.
     */
    public PivotedMatrixView(Matrix mtx, int[] rowPivot, int[] colPivot) {
        super(new PivotedMatrixStorage(mtx == null ? null : mtx.getStorage(), rowPivot, colPivot));

        this.mtx = mtx;
    }

    /**
     * @param mtx Parent matrix.
     */
    public PivotedMatrixView(Matrix mtx) {
        super(new PivotedMatrixStorage(mtx == null ? null : mtx.getStorage()));

        this.mtx = mtx;
    }

    /**
     * @param mtx Parent matrix.
     * @param pivot Pivot array for rows and columns.
     */
    public PivotedMatrixView(Matrix mtx, int[] pivot) {
        super(new PivotedMatrixStorage(mtx == null ? null : mtx.getStorage(), pivot));

        this.mtx = mtx;
    }

    /**
     * Swaps indexes {@code i} and {@code j} for both both row and column.
     *
     * @param i First index to swap.
     * @param j Second index to swap.
     */
    public Matrix swap(int i, int j) {
        swapRows(i, j);
        swapColumns(i, j);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix swapRows(int i, int j) {
        if (i < 0 || i >= storage().rowPivot().length)
            throw new IndexException(i);
        if (j < 0 || j >= storage().rowPivot().length)
            throw new IndexException(j);

        storage().swapRows(i, j);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Matrix swapColumns(int i, int j) {
        if (i < 0 || i >= storage().columnPivot().length)
            throw new IndexException(i);
        if (j < 0 || j >= storage().columnPivot().length)
            throw new IndexException(j);

        storage().swapColumns(i, j);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Vector viewRow(int row) {
        return new PivotedVectorView(
            mtx.viewRow(storage().rowPivot()[row]),
            storage().columnPivot(),
            storage().columnUnpivot()
        );
    }

    /** {@inheritDoc} */
    @Override public Vector viewColumn(int col) {
        return new PivotedVectorView(
            mtx.viewColumn(storage().columnPivot()[col]),
            storage().rowPivot(),
            storage().rowUnpivot()
        );
    }

    /**
     * @return Parent matrix.
     */
    public Matrix getBaseMatrix() {
        return mtx;
    }

    /**
     * @return Pivot array for rows.
     */
    public int[] rowPivot() {
        return storage().rowPivot();
    }

    /**
     * @return Pivot array for columns.
     */
    public int[] columnPivot() {
        return storage().columnPivot();
    }

    /**
     * @param i Index for row pivot.
     * @return Row pivot for given index.
     */
    public int rowPivot(int i) {
        return storage().rowPivot()[i];
    }

    /**
     * @param i Index for column pivot.
     * @return Column pivot for given index.
     */
    public int columnPivot(int i) {
        return storage().columnPivot()[i];
    }

    /**
     * @param i Index for row unpivot.
     * @return Row unpivot for given index.
     */
    public int rowUnpivot(int i) {
        return storage().rowUnpivot()[i];
    }

    /**
     * @param i Index for column unpivot.
     * @return Column unpivot for given index.
     */
    public int columnUnpivot(int i) {
        return storage().columnUnpivot()[i];
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(mtx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        mtx = (Matrix)in.readObject();
    }

    /** */
    private PivotedMatrixStorage storage() {
        return (PivotedMatrixStorage)getStorage();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        return new PivotedMatrixView(mtx, storage().rowPivot(), storage().columnPivot());
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + mtx.hashCode();
        res = res * 37 + getStorage().hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PivotedMatrixView that = (PivotedMatrixView)o;

        MatrixStorage sto = storage();

        return mtx.equals(that.mtx) && sto.equals(that.storage());
    }
}

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

package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.math.*;
import org.apache.ignite.math.exceptions.*;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.storage.matrix.PivotedMatrixStorage;
import org.apache.ignite.math.impls.vector.PivotedVectorView;
import java.io.*;

/**
 * Pivoted (index mapped) view over another matrix implementation.
 */
public class PivotedMatrixView extends AbstractMatrix {
    private Matrix mtx;

    /**
     * 
     */
    public PivotedMatrixView() {
        // No-op.
    }

    /**
     *
     * @param mtx
     * @param rowPivot
     * @param colPivot
     */
    public PivotedMatrixView(Matrix mtx, int[] rowPivot, int[] colPivot) {
        super(new PivotedMatrixStorage(mtx.getStorage(), rowPivot, colPivot));

        this.mtx = mtx;
    }

    /**
     *
     * @param mtx
     */
    public PivotedMatrixView(Matrix mtx) {
        super(new PivotedMatrixStorage(mtx.getStorage()));

        this.mtx = mtx;
    }

    /**
     *
     * @param mtx
     * @param pivot
     */
    public PivotedMatrixView(Matrix mtx, int[] pivot) {
        super(new PivotedMatrixStorage(mtx.getStorage(), pivot));

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

    @Override public Matrix swapRows(int i, int j) {
        if (i < 0 || i >= storage().rowPivot().length)
            throw new IndexException(i);
        if (j < 0 || j >= storage().rowPivot().length)
            throw new IndexException(j);

        storage().swapRows(i, j);

        return this;
    }

    @Override public Matrix swapColumns(int i, int j) {
        if (i < 0 || i >= storage().columnPivot().length)
            throw new IndexException(i);
        if (j < 0 || j >= storage().columnPivot().length)
            throw new IndexException(j);

        storage().swapColumns(i, j);

        return this;
    }

    @Override public Vector viewRow(int row) {
        return new PivotedVectorView(
            mtx.viewRow(storage().rowPivot()[row]),
            storage().columnPivot(),
            storage().columnUnpivot()
        );
    }

    @Override public Vector viewColumn(int col) {
        return new PivotedVectorView(
            mtx.viewColumn(storage().columnPivot()[col]),
            storage().rowPivot(),
            storage().rowUnpivot()
        );
    }

    /**
     *
     * @return
     */
    public Matrix getBaseMatrix() {
        return mtx;
    }

    /**
     *
     * @return
     */
    public int[] rowPivot() {
        return storage().rowPivot();
    }

    /**
     * 
     * @return
     */
    public int[] columnPivot() {
        return storage().columnPivot();
    }

    /**
     *
     * @param i
     * @return
     */
    public int rowPivot(int i) {
        return storage().rowPivot()[i];
    }

    /**
     *
     * @param i
     * @return
     */
    public int columnPivot(int i) {
        return storage().columnPivot()[i];
    }

    /**
     *
     * @param i
     * @return
     */
    public int rowUnpivot(int i) {
        return storage().rowUnpivot()[i];
    }

    /**
     * 
     * @param i
     * @return
     */
    public int columnUnpivot(int i) {
        return storage().columnUnpivot()[i];
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(mtx);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        mtx = (Matrix)in.readObject();
    }

    /**
     *
     * @return
     */
    private PivotedMatrixStorage storage() {
        return (PivotedMatrixStorage)getStorage();
    }

    @Override public Matrix copy() {
        return new PivotedMatrixView(mtx, storage().rowPivot(), storage().columnPivot());
    }

    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }
}

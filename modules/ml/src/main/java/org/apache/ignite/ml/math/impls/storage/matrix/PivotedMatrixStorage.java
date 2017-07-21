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

package org.apache.ignite.ml.math.impls.storage.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import org.apache.ignite.ml.math.MatrixStorage;

/**
 * Pivoted (index mapped) view over another matrix storage implementation.
 */
public class PivotedMatrixStorage implements MatrixStorage {
    /** Matrix storage. */
    private MatrixStorage sto;

    /** */
    private int[] rowPivot;
    /** */
    private int[] colPivot;
    /** */
    private int[] rowUnpivot;
    /** */
    private int[] colUnpivot;

    /**
     *
     */
    public PivotedMatrixStorage() {
        // No-op.
    }

    /**
     * @param sto Matrix storage.
     * @param rowPivot Pivot array for rows.
     * @param colPivot Pivot array for columns.
     */
    public PivotedMatrixStorage(MatrixStorage sto, int[] rowPivot, int[] colPivot) {
        assert sto != null;
        assert rowPivot != null;
        assert colPivot != null;

        this.sto = sto;
        this.rowPivot = rowPivot;
        this.colPivot = colPivot;

        rowUnpivot = invert(rowPivot);
        colUnpivot = invert(colPivot);
    }

    /**
     *
     */
    public int[] rowPivot() {
        return rowPivot;
    }

    /**
     *
     */
    public int[] columnPivot() {
        return colPivot;
    }

    /**
     *
     */
    public int[] rowUnpivot() {
        return rowUnpivot;
    }

    /**
     *
     */
    public int[] columnUnpivot() {
        return colUnpivot;
    }

    /**
     * @param sto Matrix storage.
     * @param pivot Pivot array.
     */
    public PivotedMatrixStorage(MatrixStorage sto, int[] pivot) {
        this(sto, pivot, pivot == null ? null : java.util.Arrays.copyOf(pivot, pivot.length));
    }

    /**
     * @param sto Matrix storage.
     */
    public PivotedMatrixStorage(MatrixStorage sto) {
        this(sto, sto == null ? null : identityPivot(sto.rowSize()), sto == null ? null : identityPivot(sto.columnSize()));
    }

    /**
     * @param i First row index to swap.
     * @param j Second row index to swap.
     */
    public void swapRows(int i, int j) {
        if (i != j) {
            int tmp = rowPivot[i];

            rowPivot[i] = rowPivot[j];
            rowPivot[j] = tmp;

            rowUnpivot[rowPivot[i]] = i;
            rowUnpivot[rowPivot[j]] = j;
        }
    }

    /**
     * @param i First column index to swap.
     * @param j Second column index to swap.
     */
    public void swapColumns(int i, int j) {
        if (i != j) {
            int tmp = colPivot[i];

            colPivot[i] = colPivot[j];
            colPivot[j] = tmp;

            colUnpivot[colPivot[i]] = i;
            colUnpivot[colPivot[j]] = j;
        }
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return sto.get(rowPivot[x], colPivot[y]);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        sto.set(rowPivot[x], colPivot[y], v);
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeObject(rowPivot);
        out.writeObject(colPivot);
        out.writeObject(rowUnpivot);
        out.writeObject(colUnpivot);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (MatrixStorage)in.readObject();
        rowPivot = (int[])in.readObject();
        colPivot = (int[])in.readObject();
        rowUnpivot = (int[])in.readObject();
        colUnpivot = (int[])in.readObject();
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
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + sto.hashCode();
        res = res * 37 + Arrays.hashCode(rowPivot);
        res = res * 37 + Arrays.hashCode(rowUnpivot);
        res = res * 37 + Arrays.hashCode(colPivot);
        res = res * 37 + Arrays.hashCode(colUnpivot);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        PivotedMatrixStorage that = (PivotedMatrixStorage)obj;

        return Arrays.equals(rowPivot, that.rowPivot) && Arrays.equals(rowUnpivot, that.rowUnpivot)
            && Arrays.equals(colPivot, that.colPivot) && Arrays.equals(colUnpivot, that.colUnpivot)
            && (sto != null ? sto.equals(that.sto) : that.sto == null);
    }

    /**
     * @param n Pivot array length.
     */
    private static int[] identityPivot(int n) {
        int[] pivot = new int[n];

        for (int i = 0; i < n; i++)
            pivot[i] = i;

        return pivot;
    }

    /**
     * @param pivot Pivot array to be inverted.
     */
    private static int[] invert(int[] pivot) {
        int[] x = new int[pivot.length];

        for (int i = 0; i < pivot.length; i++)
            x[pivot[i]] = i;

        return x;
    }
}

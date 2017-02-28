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

import org.apache.ignite.math.*;
import java.io.*;

/**
 * Pivoted (index mapped) view over another matrix storage implementation.
 */
public class PivotedMatrixStorage implements MatrixStorage {
    private MatrixStorage sto;

    private int[] rowPivot, colPivot;
    private int[] rowUnpivot, colUnpivot;

    /**
     *
     */
    public PivotedMatrixStorage() {
        // No-op.
    }

    /**
     *
     * @param sto
     * @param rowPivot
     * @param colPivot
     */
    public PivotedMatrixStorage(MatrixStorage sto, int[] rowPivot, int[] colPivot) {
        this.sto = sto;
        this.rowPivot = rowPivot;
        this.colPivot = colPivot;

        rowUnpivot = invert(rowPivot);
        colUnpivot = invert(colPivot);
    }

    /**
     *
     * @return
     */
    public int[] rowPivot() {
        return rowPivot;
    }

    /**
     *
     * @return
     */
    public int[] columnPivot() {
        return colPivot;
    }

    /**
     *
     * @return
     */
    public int[] rowUnpivot() {
        return rowUnpivot;
    }

    /**
     * 
     * @return
     */
    public int[] columnUnpivot() {
        return colUnpivot;
    }

    /**
     *
     * @param sto
     * @param pivot
     */
    public PivotedMatrixStorage(MatrixStorage sto, int[] pivot) {
        this(sto, pivot, java.util.Arrays.copyOf(pivot, pivot.length));
    }

    /**
     * 
     * @param sto
     */
    public PivotedMatrixStorage(MatrixStorage sto) {
        this(sto, identityPivot(sto.rowSize()),identityPivot(sto.columnSize()));
    }

    /**
     *
     * @param i
     * @param j
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
     *
     * @param i
     * @param j
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

    @Override
    public double get(int x, int y) {
        return sto.get(rowPivot[x], colPivot[y]);
    }

    @Override
    public void set(int x, int y, double v) {
        sto.set(rowPivot[x], colPivot[y], v);
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
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeObject(rowPivot);
        out.writeObject(colPivot);
        out.writeObject(rowUnpivot);
        out.writeObject(colUnpivot);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (MatrixStorage)in.readObject();
        rowPivot = (int[])in.readObject();
        colPivot = (int[])in.readObject();
        rowUnpivot = (int[])in.readObject();
        colUnpivot = (int[])in.readObject();
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
     *
     * @param n
     * @return
     */
    private static int[] identityPivot(int n) {
        int[] pivot = new int[n];

        for (int i = 0; i < n; i++)
            pivot[i] = i;

        return pivot;
    }

    /**
     * 
     * @param pivot
     * @return
     */
    private static int[] invert(int[] pivot) {
        int[] x = new int[pivot.length];

        for (int i = 0; i < pivot.length; i++)
            x[pivot[i]] = i;
        
        return x;
    }
}

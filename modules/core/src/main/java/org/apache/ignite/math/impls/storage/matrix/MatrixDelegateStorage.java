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
 * TODO: add description.
 */
public class MatrixDelegateStorage implements MatrixStorage {
    private MatrixStorage sto;
    private int rowOff, colOff;
    private int rows, cols;

    /**
     *
     */
    public MatrixDelegateStorage() {
        // No-op.
    }

    /**
     *
     * @param sto
     * @param rowOff
     * @param colOff
     * @param rows
     * @param cols
     */
    public MatrixDelegateStorage(MatrixStorage sto, int rowOff, int colOff, int rows, int cols) {
        assert sto != null;
        assert rowOff >= 0;
        assert colOff >= 0;
        assert rows > 0;
        assert cols > 0;
        
        this.sto = sto;

        this.rowOff = rowOff;
        this.colOff = colOff;

        this.rows = rows;
        this.cols = cols;
    }

    /**
     * 
     * @return
     */
    public MatrixStorage delegate() {
        return sto;
    }

    /**
     *
     * @return
     */
    public int rowOffset() {
        return rowOff;
    }

    /**
     *
     * @return
     */
    public int columnOffset() {
        return colOff;
    }

    /**
     *
     * @return
     */
    public int rowsLength() {
        return rows;
    }

    /**
     *
     * @return
     */
    public int columnsLength() {
        return cols;
    }

    @Override public double get(int x, int y) {
        return sto.get(rowOff + x, colOff + y);
    }

    @Override public void set(int x, int y, double v) {
        sto.set(rowOff + x, colOff + y, v);
    }

    @Override public int columnSize() {
        return cols;
    }

    @Override public int rowSize() {
        return rows;
    }

    @Override public boolean isArrayBased() {
        return sto.isArrayBased() && rowOff == 0 && colOff == 0;
    }

    @Override public boolean isSequentialAccess() {
        return sto.isSequentialAccess();
    }

    @Override public boolean isDense() {
        return sto.isDense();
    }

    @Override public double getLookupCost() {
        return sto.getLookupCost();
    }

    @Override public boolean isAddConstantTime() {
        return sto.isAddConstantTime();
    }

    @Override public double[][] data() {
        return sto.data();
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);

        out.writeInt(rowOff);
        out.writeInt(colOff);

        out.writeInt(rows);
        out.writeInt(cols);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (MatrixStorage)in.readObject();

        rowOff = in.readInt();
        colOff = in.readInt();

        rows = in.readInt();
        cols = in.readInt();
    }
}

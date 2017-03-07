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
import org.apache.ignite.math.UnsupportedOperationException;
import java.io.*;

/**
 * Read-only or read-write function-based matrix storage.
 */
public class FunctionMatrixStorage implements MatrixStorage {
    private int rows, cols;

    private IntIntToDoubleFunction getFunc;
    private IntIntDoubleToVoidFunction setFunc;

    /**
     *
     */
    public FunctionMatrixStorage() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     * @param getFunc
     * @param setFunc
     */
    public FunctionMatrixStorage(int rows, int cols, IntIntToDoubleFunction getFunc, IntIntDoubleToVoidFunction setFunc) {
        assert rows > 0;
        assert cols > 0;
        assert getFunc != null;

        this.rows = rows;
        this.cols = cols;
        this.getFunc = getFunc;
        this.setFunc = setFunc;
    }

    /**
     *
     * @param rows
     * @param cols
     * @param getFunc
     */
    public FunctionMatrixStorage(int rows, int cols, IntIntToDoubleFunction getFunc) {
        this(rows, cols, getFunc, null);
    }

    @Override public double get(int x, int y) {
        return getFunc.apply(x, y);
    }

    @Override public void set(int x, int y, double v) {
        if (setFunc != null)
            setFunc.apply(x, y, v);
        else
            throw new UnsupportedOperationException("Cannot set into read-only matrix.");
    }

    /**
     * 
     * @return
     */
    public IntIntToDoubleFunction getFunction() {
        return getFunc;
    }

    /**
     *
     * @return
     */
    public IntIntDoubleToVoidFunction setFunction() {
        return setFunc;
    }

    @Override public int columnSize() {
        return cols;
    }

    @Override public int rowSize() {
        return rows;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(setFunc);
        out.writeObject(getFunc);
        out.writeInt(rows);
        out.writeInt(cols);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setFunc = (IntIntDoubleToVoidFunction)in.readObject();
        getFunc = (IntIntToDoubleFunction)in.readObject();
        rows = in.readInt();
        cols = in.readInt();
    }

    @Override public boolean isSequentialAccess() {
        return false;
    }

    @Override public boolean isDense() {
        return false;
    }

    @Override public double getLookupCost() {
        return 0;
    }

    @Override public boolean isAddConstantTime() {
        return false;
    }

    @Override public boolean isArrayBased() {
        return false;
    }
}

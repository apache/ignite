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
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IntIntDoubleToVoidFunction;
import org.apache.ignite.ml.math.functions.IntIntToDoubleFunction;

/**
 * Read-only or read-write function-based matrix storage.
 */
public class FunctionMatrixStorage implements MatrixStorage {
    /** */
    private int rows;
    /** */
    private int cols;

    /** */
    private IntIntToDoubleFunction getFunc;
    /** */
    private IntIntDoubleToVoidFunction setFunc;

    /**
     *
     */
    public FunctionMatrixStorage() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     * @param getFunc Function that returns value corresponding to given row and column index.
     * @param setFunc Set function. If {@code null} - this will be a read-only matrix.
     */
    public FunctionMatrixStorage(int rows, int cols, IntIntToDoubleFunction getFunc,
        IntIntDoubleToVoidFunction setFunc) {
        assert rows > 0;
        assert cols > 0;
        assert getFunc != null;

        this.rows = rows;
        this.cols = cols;
        this.getFunc = getFunc;
        this.setFunc = setFunc;
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     * @param getFunc Function that returns value corresponding to given row and column index.
     */
    public FunctionMatrixStorage(int rows, int cols, IntIntToDoubleFunction getFunc) {
        this(rows, cols, getFunc, null);
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return getFunc.apply(x, y);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        if (setFunc != null)
            setFunc.apply(x, y, v);
        else
            throw new UnsupportedOperationException("Cannot set into read-only matrix.");
    }

    /**
     * @return Getter function.
     */
    public IntIntToDoubleFunction getFunction() {
        return getFunc;
    }

    /**
     * @return Setter function.
     */
    public IntIntDoubleToVoidFunction setFunction() {
        return setFunc;
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(setFunc);
        out.writeObject(getFunc);
        out.writeInt(rows);
        out.writeInt(cols);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setFunc = (IntIntDoubleToVoidFunction)in.readObject();
        getFunc = (IntIntToDoubleFunction)in.readObject();
        rows = in.readInt();
        cols = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        FunctionMatrixStorage that = (FunctionMatrixStorage)o;

        return rows == that.rows && cols == that.cols
            && (getFunc != null ? getFunc.equals(that.getFunc) : that.getFunc == null)
            && (setFunc != null ? setFunc.equals(that.setFunc) : that.setFunc == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = rows;

        res = 31 * res + cols;
        res = 31 * res + (getFunc != null ? getFunc.hashCode() : 0);
        res = 31 * res + (setFunc != null ? setFunc.hashCode() : 0);

        return res;
    }
}

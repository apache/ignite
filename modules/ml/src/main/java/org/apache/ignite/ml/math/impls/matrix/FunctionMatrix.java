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

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IntIntDoubleToVoidFunction;
import org.apache.ignite.ml.math.functions.IntIntToDoubleFunction;
import org.apache.ignite.ml.math.impls.storage.matrix.FunctionMatrixStorage;

/**
 * Implementation of {@link Matrix} that maps row and column index to {@link java.util.function} interfaces.
 */
public class FunctionMatrix extends AbstractMatrix {
    /**
     *
     */
    public FunctionMatrix() {
        // No-op.
    }

    /**
     * Creates read-write or read-only function matrix.
     *
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     * @param getFunc Function that returns value corresponding to given row and column index.
     * @param setFunc Set function. If {@code null} - this will be a read-only matrix.
     */
    public FunctionMatrix(int rows, int cols, IntIntToDoubleFunction getFunc, IntIntDoubleToVoidFunction setFunc) {
        assert rows > 0;
        assert cols > 0;
        assert getFunc != null;

        setStorage(new FunctionMatrixStorage(rows, cols, getFunc, setFunc));
    }

    /**
     * Creates read-only function matrix.
     *
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     * @param getFunc Function that returns value corresponding to given row and column index.
     */
    public FunctionMatrix(int rows, int cols, IntIntToDoubleFunction getFunc) {
        assert rows > 0;
        assert cols > 0;
        assert getFunc != null;

        setStorage(new FunctionMatrixStorage(rows, cols, getFunc));
    }

    /**
     *
     *
     */
    private FunctionMatrixStorage storage() {
        return (FunctionMatrixStorage)getStorage();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        FunctionMatrixStorage sto = storage();

        return new FunctionMatrix(sto.rowSize(), sto.columnSize(), sto.getFunction(), sto.setFunction());
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        FunctionMatrixStorage sto = storage();

        return new FunctionMatrix(rows, cols, sto.getFunction(), sto.setFunction());
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }
}

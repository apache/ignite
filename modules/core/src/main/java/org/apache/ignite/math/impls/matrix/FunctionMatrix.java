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
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.matrix.FunctionMatrixStorage;

import java.util.*;

/**
 * TODO: add description.
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
     * @param rows
     * @param cols
     * @param getFunc
     * @param setFunc Set function. If {@code null} - this will be a read-only matrix.
     */
    public FunctionMatrix(int rows, int cols, IntIntToDoubleFunction getFunc, IntIntDoubleToVoidFunction setFunc) {
        setStorage(new FunctionMatrixStorage(rows, cols, getFunc, setFunc));
    }

    /**
     * Creates read-only function matrix.
     *
     * @param rows
     * @param cols
     * @param getFunc
     */
    public FunctionMatrix(int rows, int cols, IntIntToDoubleFunction getFunc) {
        setStorage(new FunctionMatrixStorage(rows, cols, getFunc));
    }

    /**
     * @param args
     */
    public FunctionMatrix(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("rows") && args.containsKey("cols") &&
            args.containsKey("getFunc") && args.containsKey("setFunc")) {
            IntIntToDoubleFunction getFunc = (IntIntToDoubleFunction)args.get("getFunc");
            IntIntDoubleToVoidFunction setFunc = (IntIntDoubleToVoidFunction)args.get("setFunc");
            int rows = (int)args.get("rows");
            int cols = (int)args.get("cols");

            setStorage(new FunctionMatrixStorage(rows, cols, getFunc, setFunc));
        }
        else if (args.containsKey("rows") && args.containsKey("cols") &&
            args.containsKey("getFunc") && args.containsKey("setFunc")) {
            IntIntToDoubleFunction getFunc = (IntIntToDoubleFunction)args.get("getFunc");
            int rows = (int)args.get("rows");
            int cols = (int)args.get("cols");

            setStorage(new FunctionMatrixStorage(rows, cols, getFunc));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     * @return
     */
    private FunctionMatrixStorage storage() {
        return (FunctionMatrixStorage)getStorage();
    }

    @Override
    public Matrix copy() {
        FunctionMatrixStorage sto = storage();

        return new FunctionMatrix(sto.rowSize(), sto.columnSize(), sto.getFunction(), sto.setFunction());
    }

    @Override
    public Matrix like(int rows, int cols) {
        FunctionMatrixStorage sto = storage();

        return new FunctionMatrix(rows, cols, sto.getFunction(), sto.setFunction());
    }

    @Override
    public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }
}

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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.impls.storage.*;
import java.util.*;

/**
 * Basic implementation for matrix.
 * 
 * This is a trivial implementation for matrix assuming dense logic, local on-heap JVM storage
 * based on <code>double[][]</code> array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseLocalOnHeapMatrix extends AbstractMatrix {
    /** */
    private final int DFLT_ROWS_SIZE = 100;
    /** */
    private final int DFLT_COLS_SIZE = 100;

    /**
     *
     */
    public DenseLocalOnHeapMatrix() {
        setStorage(new MatrixArrayStorage(DFLT_ROWS_SIZE, DFLT_COLS_SIZE));
    }

   /**
     *
     * @param rows
     * @param cols
     */
    public DenseLocalOnHeapMatrix(int rows, int cols) {
        setStorage(new MatrixArrayStorage(rows, cols));
    }

    /**
     *
     * @param mtx
     */
    public DenseLocalOnHeapMatrix(double[][] mtx) {
        setStorage(new MatrixArrayStorage(mtx));
    }

    /**
     *
     * @param orig
     */
    private DenseLocalOnHeapMatrix(DenseLocalOnHeapMatrix orig) {
        setStorage(new MatrixArrayStorage(orig.rowSize(), orig.columnSize()));

        assign(orig);
    }

    /**
     *
     * @param args
     */
    public DenseLocalOnHeapMatrix(Map<String, Object> args) {
        if (args == null)
            setStorage(new MatrixArrayStorage(DFLT_ROWS_SIZE, DFLT_COLS_SIZE));
        else if (args.containsKey("rows") && args.containsKey("cols"))
            setStorage(new MatrixArrayStorage((int)args.get("rows"), (int)args.get("cols")));
        else if (args.containsKey("mtx"))
            setStorage(new MatrixArrayStorage((double[][])args.get("mtx")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    @Override
    public Matrix copy() {
        return new DenseLocalOnHeapMatrix(this);
    }

    @Override
    public Matrix like(int rows, int cols) {
        return new DenseLocalOnHeapMatrix(rows, cols);
    }
}

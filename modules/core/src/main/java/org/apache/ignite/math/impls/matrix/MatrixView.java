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
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.storage.matrix.MatrixDelegateStorage;

import java.io.*;

/**
 * TODO: add description.
 */
public class MatrixView extends AbstractMatrix {
    /**
     * Constructor for {@link Externalizable} interface.
     */
    public MatrixView(){
        // No-op.
    }

    /**
     * 
     * @param parent
     * @param rowOff
     * @param colOff
     * @param rows
     * @param cols
     */
    public MatrixView(Matrix parent, int rowOff, int colOff, int rows, int cols) {
        this(parent.getStorage(), rowOff, colOff, rows, cols);
    }

    /**
     *
     * @param sto
     * @param rowOff
     * @param colOff
     * @param rows
     * @param cols
     */
    public MatrixView(MatrixStorage sto, int rowOff, int colOff, int rows, int cols) {
        super(new MatrixDelegateStorage(sto, rowOff, colOff, rows, cols));
    }

    /**
     * 
     * @return
     */
    private MatrixDelegateStorage storage() {
        return (MatrixDelegateStorage)getStorage();
    }

    @Override public Matrix copy() {
        MatrixDelegateStorage sto = storage();

        return new MatrixView(sto.delegate(), sto.rowOffset(), sto.columnOffset(), sto.rowSize(), sto.columnSize());
    }

    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }
}

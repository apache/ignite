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

package org.apache.ignite.ml.math.primitives.matrix.impl;

import java.io.Externalizable;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.primitives.matrix.AbstractMatrix;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.MatrixStorage;
import org.apache.ignite.ml.math.primitives.matrix.storage.ViewMatrixStorage;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Implements the rectangular view into the parent {@link Matrix}.
 */
public class ViewMatrix extends AbstractMatrix {
    /**
     * Constructor for {@link Externalizable} interface.
     */
    public ViewMatrix() {
        // No-op.
    }

    /**
     * @param parent Backing parent {@link Matrix}.
     * @param rowOff Row offset to parent matrix.
     * @param colOff Column offset to parent matrix.
     * @param rows Amount of rows in the view.
     * @param cols Amount of columns in the view.
     */
    public ViewMatrix(Matrix parent, int rowOff, int colOff, int rows, int cols) {
        this(parent == null ? null : parent.getStorage(), rowOff, colOff, rows, cols);
    }

    /**
     * @param sto Backing parent {@link MatrixStorage}.
     * @param rowOff Row offset to parent storage.
     * @param colOff Column offset to parent storage.
     * @param rows Amount of rows in the view.
     * @param cols Amount of columns in the view.
     */
    public ViewMatrix(MatrixStorage sto, int rowOff, int colOff, int rows, int cols) {
        super(new ViewMatrixStorage(sto, rowOff, colOff, rows, cols));
    }

    /**
     *
     *
     */
    private ViewMatrixStorage storage() {
        return (ViewMatrixStorage)getStorage();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        ViewMatrixStorage sto = storage();

        return new ViewMatrix(sto.delegate(), sto.rowOffset(), sto.columnOffset(), sto.rowSize(), sto.columnSize());
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }
}

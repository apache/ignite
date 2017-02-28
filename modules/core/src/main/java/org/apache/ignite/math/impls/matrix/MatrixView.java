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
import org.apache.ignite.math.impls.storage.matrix.MatrixDelegateStorage;

import java.io.*;

/**
 * TODO: add description.
 */
public class MatrixView extends AbstractMatrix {
    private Matrix parent;

    private int rowOff;
    private int colOff;

    private int rows;
    private int cols;

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
        super(new MatrixDelegateStorage(parent.getStorage(), rowOff, colOff, rows, cols));

        this.parent = parent;

        this.rowOff = rowOff;
        this.colOff = colOff;

        this.rows = rows;
        this.cols = cols;
    }

    @Override
    public Matrix copy() {
        return new MatrixView(parent, rowOff, colOff, rows, cols);
    }

    @Override
    public Matrix like(int rows, int cols) {
        return parent.like(rows, cols);
    }

    @Override
    public Vector likeVector(int crd) {
        return parent.likeVector(crd);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(parent);
        out.writeInt(rowOff);
        out.writeInt(colOff);
        out.writeInt(rows);
        out.writeInt(cols);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        
        parent = (Matrix)in.readObject();
        rowOff = in.readInt();
        colOff = in.readInt();
        rows = in.readInt();
        cols = in.readInt();
    }
}

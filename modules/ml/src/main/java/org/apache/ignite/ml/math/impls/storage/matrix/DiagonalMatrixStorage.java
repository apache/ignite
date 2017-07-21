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
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;

/**
 * {@link MatrixStorage} implementation for diagonal Matrix view.
 */
public class DiagonalMatrixStorage implements MatrixStorage {
    /** Backing vector for matrix diagonal. */
    private Vector diagonal;

    /**
     *
     */
    public DiagonalMatrixStorage() {
        // No-op.
    }

    /**
     * @param diagonal Backing {@link Vector} for matrix diagonal.
     */
    public DiagonalMatrixStorage(Vector diagonal) {
        assert diagonal != null;

        this.diagonal = diagonal;
    }

    /**
     *
     */
    public Vector diagonal() {
        return diagonal;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return x == y ? diagonal.get(x) : 0.0;
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        if (x == y)
            diagonal.set(x, v);
        else
            throw new UnsupportedOperationException("Can't set off-diagonal element.");
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return diagonal.size();
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return diagonal.size();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(diagonal);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        diagonal = (Vector)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return diagonal.isSequentialAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return diagonal.isDense();
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return diagonal.isRandomAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return diagonal.isDistributed();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + diagonal.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DiagonalMatrixStorage that = (DiagonalMatrixStorage)o;

        return diagonal.equals(that.diagonal);
    }
}

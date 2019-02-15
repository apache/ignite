/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.math.primitives.vector.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.ml.math.exceptions.IndexException;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.AbstractVector;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.storage.VectorizedViewMatrixStorage;

/**
 * Row or column vector view off the matrix.
 */
public class VectorizedViewMatrix extends AbstractVector {
    /** */
    private Matrix parent;

    /** */
    private int row;
    /** */
    private int col;

    /** */
    private int rowStride;
    /** */
    private int colStride;

    /**
     *
     */
    public VectorizedViewMatrix() {
        // No-op.
    }

    /**
     * @param parent Parent matrix.
     * @param row Starting row in the view.
     * @param col Starting column in the view.
     * @param rowStride Rows stride in the view.
     * @param colStride Columns stride in the view.
     */
    public VectorizedViewMatrix(Matrix parent, int row, int col, int rowStride, int colStride) {
        assert parent != null;

        if (row < 0 || row >= parent.rowSize())
            throw new IndexException(row);
        if (col < 0 || col >= parent.columnSize())
            throw new IndexException(col);

        this.parent = parent;

        this.row = row;
        this.col = col;

        this.rowStride = rowStride;
        this.colStride = colStride;

        setStorage(new VectorizedViewMatrixStorage(parent, row, col, rowStride, colStride));
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return new VectorizedViewMatrix(parent, row, col, rowStride, colStride);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return parent.likeVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return parent.like(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(parent);
        out.writeInt(row);
        out.writeInt(col);
        out.writeInt(rowStride);
        out.writeInt(colStride);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        parent = (Matrix)in.readObject();
        row = in.readInt();
        col = in.readInt();
        rowStride = in.readInt();
        colStride = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + (parent == null ? 0 : parent.hashCode());
        res = res * 37 + row;
        res = res * 37 + col;
        res = res * 37 + rowStride;
        res = res * 37 + colStride;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        VectorizedViewMatrix that = (VectorizedViewMatrix)o;

        return (parent != null ? parent.equals(that.parent) : that.parent == null) &&
            row == that.row &&
            col == that.col &&
            rowStride == that.rowStride &&
            colStride == that.colStride;
    }
}

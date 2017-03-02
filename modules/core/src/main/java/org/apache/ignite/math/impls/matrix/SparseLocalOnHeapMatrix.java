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

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.vector.SparseLocalOnHeapVector;
import org.apache.ignite.math.impls.storage.matrix.SparseLocalMatrixStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Sparse local onheap matrix with {@link SparseLocalOnHeapVector} as rows.
 */
public class SparseLocalOnHeapMatrix extends AbstractMatrix {
    private int accMode = SparseDistributedMatrix.RANDOM_ACCESS_MODE;
    private int stoMode = AbstractMatrix.ROW_STORAGE_MODE;
    /** */
    public SparseLocalOnHeapMatrix(){
        // No-op
    }

    /** */
    public SparseLocalOnHeapMatrix(int rows, int cols){
        this(rows, cols, SparseDistributedMatrix.RANDOM_ACCESS_MODE);
    }

    /** */
    public SparseLocalOnHeapMatrix(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("accessMode"))
            accMode = (int) args.get("accessMode");

        if (args.containsKey("rows") && args.containsKey("cols"))
            setStorage(new SparseLocalMatrixStorage((int)args.get("rows"), (int)args.get("cols"), accMode));
        else if (args.containsKey("arr"))
            setStorage(new SparseLocalMatrixStorage((double[][])args.get("arr")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    public SparseLocalOnHeapMatrix(int row, int col, int accessMode) {
        setStorage(new SparseLocalMatrixStorage(row, col, accessMode));

        accMode = accessMode;
    }

    /** {@inheritDoc} */
    @Override
    public Matrix copy() {
        Matrix copy = like(rowSize(), columnSize());
        copy.assign(this);

        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public Matrix like(int rows, int cols) {
        return new SparseLocalOnHeapMatrix(rows, cols, accMode);
    }

    /** {@inheritDoc} */
    @Override
    public Vector likeVector(int crd) {
        return new SparseLocalOnHeapVector(crd, accMode);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(accMode);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        accMode = in.readInt();
    }
}

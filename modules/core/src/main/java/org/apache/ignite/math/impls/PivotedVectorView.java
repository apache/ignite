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
import java.io.*;

/**
 * Pivoted (index mapped) view over another vector.
 */
public class PivotedVectorView extends AbstractVector {
    private Vector vec;
    /**
     *
     * @param vec
     * @param pivot
     * @param unpivot
     */
    public PivotedVectorView(Vector vec, int[] pivot, int[] unpivot) {
        this.vec = vec;

        setStorage(new PivotedVectorStorage(vec.getStorage(), pivot, unpivot));
    }

    /**
     *
     * @param vec
     * @param pivot
     */
    public PivotedVectorView(Vector vec, int[] pivot) {
        this.vec = vec;

        setStorage(new PivotedVectorStorage(vec.getStorage(), pivot));
    }

    private PivotedVectorStorage storage() {
        return (PivotedVectorStorage)getStorage();
    }

    /**
     *
     */
    public PivotedVectorView() {
        // No-op.
    }

    @Override
    public Vector copy() {
        PivotedVectorStorage sto = storage();

        return new PivotedVectorView(vec, sto.pivot(), sto.unpivot());
    }

    @Override
    public Vector like(int crd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }

    @Override
    public Matrix toMatrix(boolean row) {
        return null; // TODO
    }

    @Override
    public Matrix toMatrixPlusOne(boolean row, double zeroVal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out); // TODO
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in); // TODO
    }
}

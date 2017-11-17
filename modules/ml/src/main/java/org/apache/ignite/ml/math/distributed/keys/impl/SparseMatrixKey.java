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

package org.apache.ignite.ml.math.distributed.keys.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;

/**
 * Key implementation for {@link SparseDistributedMatrix}.
 */
public class SparseMatrixKey implements RowColMatrixKey, Externalizable {
    /** */
    private int idx;
    /** */
    private UUID matrixId;
    /** */
    @AffinityKeyMapped
    private Object affinityKey;

    /**
     * Default constructor (required by Externalizable).
     */
    public SparseMatrixKey(){

    }

    /**
     * Build Key.
     */
    public SparseMatrixKey(int idx, UUID matrixId, Object affinityKey) {
        assert idx >= 0 : "Index must be positive.";
        assert matrixId != null : "Matrix id can`t be null.";

        this.idx = idx;
        this.matrixId = matrixId;
        this.affinityKey = affinityKey;
    }

    /** {@inheritDoc} */
    @Override public int index() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public UUID dataStructureId() {
        return matrixId;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey() {
        return affinityKey;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(matrixId);
        out.writeObject(affinityKey);
        out.writeInt(idx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        matrixId = (UUID)in.readObject();
        affinityKey = in.readObject();
        idx = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = idx;
        res = 31 * res + (matrixId != null ? matrixId.hashCode() : 0);
        res = 31 * res + (affinityKey != null ? affinityKey.hashCode() : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || obj.getClass() != getClass())
            return false;

        SparseMatrixKey that = (SparseMatrixKey)obj;

        return idx == that.idx && matrixId.equals(that.matrixId) && F.eq(affinityKey, that.affinityKey);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SparseMatrixKey.class, this);
    }
}

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
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.impls.matrix.MatrixBlockEntry;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.jetbrains.annotations.Nullable;

/**
 * Key implementation for {@link MatrixBlockEntry} using for {@link SparseBlockDistributedMatrix}.
 */
public class MatrixBlockKey implements org.apache.ignite.ml.math.distributed.keys.MatrixBlockKey, Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;
    /** Block row ID */
    private long blockIdRow;
    /** Block col ID */
    private long blockIdCol;
    /** Matrix ID */
    private IgniteUuid matrixUuid;
    /** Block affinity key. */
    private IgniteUuid affinityKey;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public MatrixBlockKey() {
        // No-op.
    }

    /**
     * Construct matrix block key.
     *
     * @param matrixUuid Matrix uuid.
     * @param affinityKey Affinity key.
     */
    public MatrixBlockKey(long rowId, long colId, IgniteUuid matrixUuid, @Nullable IgniteUuid affinityKey) {
        assert rowId >= 0;
        assert colId >= 0;
        assert matrixUuid != null;

        this.blockIdRow = rowId;
        this.blockIdCol = colId;
        this.matrixUuid = matrixUuid;
        this.affinityKey = affinityKey;
    }

    /** {@inheritDoc} */
    @Override public long blockRowId() {
        return blockIdRow;
    }

    /** {@inheritDoc} */
    @Override public long blockColId() {
        return blockIdCol;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid dataStructureId() {
        return matrixUuid;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid affinityKey() {
        return affinityKey;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, matrixUuid);
        U.writeGridUuid(out, affinityKey);
        out.writeLong(blockIdRow);
        out.writeLong(blockIdCol);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        matrixUuid = U.readGridUuid(in);
        affinityKey = U.readGridUuid(in);
        blockIdRow = in.readLong();
        blockIdCol = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        BinaryUtils.writeIgniteUuid(out, matrixUuid);
        BinaryUtils.writeIgniteUuid(out, affinityKey);
        out.writeLong(blockIdRow);
        out.writeLong(blockIdCol);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        matrixUuid = BinaryUtils.readIgniteUuid(in);
        affinityKey = BinaryUtils.readIgniteUuid(in);
        blockIdRow = in.readLong();
        blockIdCol = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 37;

        res += res * 37 + blockIdCol;
        res += res * 37 + blockIdRow;
        res += res * 37 + matrixUuid.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || obj.getClass() != getClass())
            return false;

        org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey that = (org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey)obj;

        return blockIdRow == that.blockIdRow && blockIdCol == that.blockIdCol && matrixUuid.equals(that.matrixUuid)
            && F.eq(affinityKey, that.affinityKey);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey.class, this);
    }


}

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
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.ml.math.impls.vector.VectorBlockEntry;
import org.jetbrains.annotations.Nullable;

/**
 * Key implementation for {@link VectorBlockEntry} using for {@link SparseBlockDistributedVector}.
 */
public class VectorBlockKey implements org.apache.ignite.ml.math.distributed.keys.VectorBlockKey, Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Block row ID */
    private long blockId;

    /** Vector ID */
    private UUID vectorUuid;

    /** Block affinity key. */
    private UUID affinityKey;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public VectorBlockKey() {
        // No-op.
    }

    /**
     * Construct vector block key.
     *
     * @param vectorUuid Vector uuid.
     * @param affinityKey Affinity key.
     */
    public VectorBlockKey(long blockId, UUID vectorUuid, @Nullable UUID affinityKey) {
        assert blockId >= 0;
        assert vectorUuid != null;

        this.blockId = blockId;
        this.vectorUuid = vectorUuid;
        this.affinityKey = affinityKey;
    }

    /** {@inheritDoc} */
    @Override public long blockId() {
        return blockId;
    }


    /** {@inheritDoc} */
    @Override public UUID dataStructureId() {
        return vectorUuid;
    }

    /** {@inheritDoc} */
    @Override public UUID affinityKey() {
        return affinityKey;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vectorUuid);
        out.writeObject(affinityKey);
        out.writeLong(blockId);

    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vectorUuid = (UUID)in.readObject();
        affinityKey = (UUID)in.readObject();
        blockId = in.readLong();

    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeUuid(vectorUuid);
        out.writeUuid(affinityKey);
        out.writeLong(blockId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        vectorUuid = in.readUuid();
        affinityKey = in.readUuid();
        blockId = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 37;

        res += res * 37 + blockId;
        res += res * 37 + vectorUuid.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || obj.getClass() != getClass())
            return false;

        VectorBlockKey that = (VectorBlockKey)obj;

        return blockId == that.blockId  && vectorUuid.equals(that.vectorUuid)
                && F.eq(affinityKey, that.affinityKey);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VectorBlockKey.class, this);
    }
}
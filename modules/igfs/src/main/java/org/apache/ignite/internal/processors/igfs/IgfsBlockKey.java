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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.binary.*;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * File's binary data block key.
 */
public final class IgfsBlockKey implements IgfsBaseBlockKey, Message, Externalizable, Binarylizable,
    Comparable<IgfsBlockKey> {
    /** */
    private static final long serialVersionUID = 0L;

    /** File system file ID. */
    @Order(0)
    IgniteUuid fileId;

    /** Block ID. */
    @Order(1)
    long blockId;

    /** Block affinity key. */
    @Order(2)
    IgniteUuid affKey;

    /** Eviction exclude flag. */
    @Order(3)
    boolean evictExclude;

    /**
     * Constructs file's binary data block key.
     *
     * @param fileId File ID.
     * @param affKey Affinity key.
     * @param evictExclude Evict exclude flag.
     * @param blockId Block ID.
     */
    public IgfsBlockKey(IgniteUuid fileId, @Nullable IgniteUuid affKey, boolean evictExclude, long blockId) {
        assert fileId != null;
        assert blockId >= 0;

        this.fileId = fileId;
        this.affKey = affKey;
        this.evictExclude = evictExclude;
        this.blockId = blockId;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgfsBlockKey() {
        // No-op.
    }

    /**
     * @return File ID.
     */
    public IgniteUuid getFileId() {
        return fileId;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid affinityKey() {
        return affKey;
    }

    /** {@inheritDoc} */
    @Override public long blockId() {
        return blockId;
    }

    /** {@inheritDoc} */
    @Override public int fileHash() {
        return fileId.hashCode();
    }

    /**
     * @return Evict exclude flag.
     */
    public boolean evictExclude() {
        return evictExclude;
    }


    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull IgfsBlockKey o) {
        int res = fileId.compareTo(o.fileId);

        if (res != 0)
            return res;

        long v1 = blockId;
        long v2 = o.blockId;

        if (v1 != v2)
            return v1 > v2 ? 1 : -1;

        if (affKey == null && o.affKey == null)
            return 0;

        if (affKey != null && o.affKey != null)
            return affKey.compareTo(o.affKey);

        return affKey != null ? -1 : 1;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeIgniteUuid(out, fileId);
        U.writeIgniteUuid(out, affKey);
        out.writeBoolean(evictExclude);
        out.writeLong(blockId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        fileId = U.readIgniteUuid(in);
        affKey = U.readIgniteUuid(in);
        evictExclude = in.readBoolean();
        blockId = in.readLong();
    }
    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter bwriter = writer.rawWriter();
        BinaryUtils.writeIgniteUuid(bwriter, fileId);
        BinaryUtils.writeIgniteUuid(bwriter, affKey);
        bwriter.writeBoolean(evictExclude);
        bwriter.writeLong(blockId);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader breader = reader.rawReader();
        fileId = BinaryUtils.readIgniteUuid(breader);
        affKey = BinaryUtils.readIgniteUuid(breader);
        evictExclude = breader.readBoolean();
        blockId = breader.readLong();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return fileId.hashCode() + (int)(blockId ^ (blockId >>> 32));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || o.getClass() != getClass())
            return false;

        IgfsBlockKey that = (IgfsBlockKey)o;

        return blockId == that.blockId && fileId.equals(that.fileId) && Objects.equals(affKey, that.affKey) &&
            evictExclude == that.evictExclude;
    }


    /** {@inheritDoc} */
    @Override public short directType() {
        return 65;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsBlockKey.class, this);
    }

}

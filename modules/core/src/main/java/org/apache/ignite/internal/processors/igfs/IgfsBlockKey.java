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

import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * File's binary data block key.
 */
@GridInternal
public final class IgfsBlockKey implements Message, Externalizable, Comparable<IgfsBlockKey> {
    /** */
    private static final long serialVersionUID = 0L;

    /** File system file ID. */
    private IgniteUuid fileId;

    /** Block ID. */
    private long blockId;

    /** Block affinity key. */
    private IgniteUuid affKey;

    /** Eviction exclude flag. */
    private boolean evictExclude;

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

    /**
     * @return Block affinity key.
     */
    public IgniteUuid affinityKey() {
        return affKey;
    }

    /**
     * @return Evict exclude flag.
     */
    public boolean evictExclude() {
        return evictExclude;
    }

    /**
     * @return Block ID.
     */
    public long getBlockId() {
        return blockId;
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
        U.writeGridUuid(out, fileId);
        U.writeGridUuid(out, affKey);
        out.writeBoolean(evictExclude);
        out.writeLong(blockId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        fileId = U.readGridUuid(in);
        affKey = U.readGridUuid(in);
        evictExclude = in.readBoolean();
        blockId = in.readLong();
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

        return blockId == that.blockId && fileId.equals(that.fileId) && F.eq(affKey, that.affKey) &&
            evictExclude == that.evictExclude;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeIgniteUuid("affKey", affKey))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("blockId", blockId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("evictExclude", evictExclude))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeIgniteUuid("fileId", fileId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                affKey = reader.readIgniteUuid("affKey");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                blockId = reader.readLong("blockId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                evictExclude = reader.readBoolean("evictExclude");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                fileId = reader.readIgniteUuid("fileId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgfsBlockKey.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 65;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsBlockKey.class, this);
    }
}

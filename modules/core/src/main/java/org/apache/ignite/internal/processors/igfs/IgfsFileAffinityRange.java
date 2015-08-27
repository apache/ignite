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

import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Affinity range.
 */
public class IgfsFileAffinityRange implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial range status, right after creation. */
    public static final int RANGE_STATUS_INITIAL = 0;

    /** Moving range state. Fragmentizer started blocks copy. */
    public static final int RANGE_STATUS_MOVING = 1;

    /** Fragmentizer finished block copy for this range. */
    public static final int RANGE_STATUS_MOVED = 2;

    /** Range affinity key. */
    private IgniteUuid affKey;

    /** {@code True} if currently being moved by fragmentizer. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int status = RANGE_STATUS_INITIAL;

    /** Range start offset (divisible by block size). */
    private long startOff;

    /** Range end offset (endOff + 1 divisible by block size). */
    private long endOff;

    /** Field kept for backward compatibility. */
    private boolean done;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsFileAffinityRange() {
        // No-op.
    }

    /**
     * @param startOff Start offset.
     * @param endOff End offset.
     * @param affKey Affinity key.
     */
    IgfsFileAffinityRange(long startOff, long endOff, IgniteUuid affKey) {
        this.startOff = startOff;
        this.endOff = endOff;
        this.affKey = affKey;
    }

    /**
     * Creates new range with updated status.
     *
     * @param other Initial range.
     * @param status Updated status.
     */
    IgfsFileAffinityRange(IgfsFileAffinityRange other, int status) {
        startOff = other.startOff;
        endOff = other.endOff;
        affKey = other.affKey;

        this.status = status;
    }

    /**
     * @return Affinity key for this range.
     */
    public IgniteUuid affinityKey() {
        return affKey;
    }

    /**
     * @return Range start offset.
     */
    public long startOffset() {
        return startOff;
    }

    /**
     * @return Range end offset.
     */
    public long endOffset() {
        return endOff;
    }

    /**
     * @param blockStartOff Block start offset to check.
     * @return {@code True} if block with given start offset belongs to this range.
     */
    public boolean belongs(long blockStartOff) {
        return blockStartOff >= startOff && blockStartOff < endOff;
    }

    /**
     * @param blockStartOff Block start offset to check.
     * @return {@code True} if block with given start offset is located before this range.
     */
    public boolean less(long blockStartOff) {
        return blockStartOff < startOff;
    }

    /**
     * @param blockStartOff Block start offset to check.
     * @return {@code True} if block with given start offset is located after this range.
     */
    public boolean greater(long blockStartOff) {
        return blockStartOff > endOff;
    }

    /**
     * @return If range is empty, i.e. has zero length.
     */
    public boolean empty() {
        return startOff == endOff;
    }

    /**
     * @return Range status.
     */
    public int status() {
        return status;
    }

    /**
     * Expands this range by given block.
     *
     * @param blockStartOff Offset of block start.
     * @param expansionSize Block size.
     */
    public void expand(long blockStartOff, int expansionSize) {
        // If we are expanding empty range.
        if (endOff == startOff) {
            assert endOff == blockStartOff : "Failed to expand range [endOff=" + endOff +
                ", blockStartOff=" + blockStartOff + ", expansionSize=" + expansionSize + ']';

            endOff += expansionSize - 1;
        }
        else {
            assert endOff == blockStartOff - 1;

            endOff += expansionSize;
        }
    }

    /**
     * Splits range into collection if smaller ranges with length equal to {@code maxSize}.
     *
     * @param maxSize Split part maximum size.
     * @return Collection of range parts.
     */
    public Collection<IgfsFileAffinityRange> split(long maxSize) {
        long len = endOff - startOff + 1;

        if (len > maxSize) {
            int size = (int)(len / maxSize + 1);

            Collection<IgfsFileAffinityRange> res = new ArrayList<>(size);

            long pos = startOff;

            while (pos < endOff + 1) {
                long end = Math.min(pos + maxSize - 1, endOff);

                IgfsFileAffinityRange part = new IgfsFileAffinityRange(pos, end, affKey);

                part.status = status;

                res.add(part);

                pos = end + 1;
            }

            return res;
        }
        else
            return Collections.singletonList(this);
    }

    /**
     * Tries to concatenate this range with a given one. If ranges are not adjacent, will return {@code null}.
     *
     * @param range Range to concatenate with.
     * @return Concatenation result or {@code null} if ranges are not adjacent.
     */
    @Nullable public IgfsFileAffinityRange concat(IgfsFileAffinityRange range) {
        if (endOff + 1 != range.startOff || !F.eq(affKey, range.affKey) || status != RANGE_STATUS_INITIAL)
            return null;

        return new IgfsFileAffinityRange(startOff, range.endOff, affKey);
    }

    /**
     * Checks if range regions are equal.
     *
     * @param other Other range to check against.
     * @return {@code True} if range regions are equal.
     */
    public boolean regionEqual(IgfsFileAffinityRange other) {
        return startOff == other.startOff && endOff == other.endOff;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, affKey);

        out.writeInt(status);

        out.writeLong(startOff);
        out.writeLong(endOff);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        affKey = U.readGridUuid(in);

        status = in.readInt();

        startOff = in.readLong();
        endOff = in.readLong();
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
                // The field 'done' was removed, but its writing preserved for compatibility reasons.
                if (!writer.writeBoolean("done", done))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("endOff", endOff))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("startOff", startOff))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeInt("status", status))
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
                // field 'done' was removed, but reading preserved for compatibility reasons.
                done = reader.readBoolean("done");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                endOff = reader.readLong("endOff");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                startOff = reader.readLong("startOff");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                status = reader.readInt("status");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgfsFileAffinityRange.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 68;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsFileAffinityRange.class, this);
    }
}

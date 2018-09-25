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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Partition update counters message.
 */
public class PartitionUpdateCountersMessage implements Message {
    /** */
    private static final int ITEM_SIZE = 4 /* partition */ + 8 /* initial counter */ + 8 /* updates count */;

    /** */
    private static final long serialVersionUID = 193442457510062844L;

    /** */
    private byte data[];

    /** */
    private int cacheId;

    /** */
    @GridDirectTransient
    private int size;

    /** */
    public PartitionUpdateCountersMessage() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param initSize Initial size.
     */
    public PartitionUpdateCountersMessage(int cacheId, int initSize) {
        assert initSize >= 1;

        this.cacheId = cacheId;
        data = new byte[initSize * ITEM_SIZE];
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Size.
     */
    public int size() {
        return size;
    }

    /**
     * @param idx Item number.
     * @return Partition number.
     */
    public int partition(int idx) {
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        long off = GridUnsafe.BYTE_ARR_OFF + idx * ITEM_SIZE;

        return GridUnsafe.getInt(data, off);
    }

    /**
     * @param idx Item number.
     * @return Partition number.
     */
    public long initialCounter(int idx){
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        long off = GridUnsafe.BYTE_ARR_OFF + idx * ITEM_SIZE + 4;

        return GridUnsafe.getLong(data, off);
    }

    /**
     * @param idx Item number.
     * @param value Initial partition counter.
     */
    public void initialCounter(int idx, long value){
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        long off = GridUnsafe.BYTE_ARR_OFF + idx * ITEM_SIZE + 4;

        GridUnsafe.putLong(data, off, value);
    }

    /**
     * @param idx Item number.
     * @return Update counter delta.
     */
    public long updatesCount(int idx){
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        long off = GridUnsafe.BYTE_ARR_OFF + idx * ITEM_SIZE + 12;

        return GridUnsafe.getLong(data, off);
    }

    /**
     * @param part Partition number.
     * @param init Init partition counter.
     * @param updatesCnt Update counter delta.
     */
    public void add(int part, long init, long updatesCnt) {
        ensureSpace(size + 1);

        long off = GridUnsafe.BYTE_ARR_OFF + size++ * ITEM_SIZE;

        GridUnsafe.putInt(data, off, part); off += 4;
        GridUnsafe.putLong(data, off, init); off += 8;
        GridUnsafe.putLong(data, off, updatesCnt);
    }

    /**
     * Clears message.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Check if there is enough space is allocated.
     *
     * @param newSize Size to ensure.
     */
    private void ensureSpace(int newSize) {
        int req = newSize * ITEM_SIZE;

        if (data.length < req)
            data = Arrays.copyOf(data, data.length << 1);
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
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("data", data, 0, size * ITEM_SIZE))
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
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                data = reader.readByteArray("data");

                if (!reader.isLastRead())
                    return false;

                size = data.length / ITEM_SIZE;

                reader.incrementState();

        }

        return reader.afterMessageRead(PartitionUpdateCountersMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 157;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < size; i++) {
            sb.append("[part=")
                .append(partition(i))
                .append(", initCntr=")
                .append(initialCounter(i))
                .append(", cntr=")
                .append(updatesCount(i))
                .append(']');
        }

        return "PartitionUpdateCountersMessage{" +
            "cacheId=" + cacheId +
            ", size=" + size +
            ", cntrs=" + sb +
            '}';
    }
}

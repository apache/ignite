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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Statistics by column (or by set of columns, if they collected together)
 */
public class StatisticsColumnData implements Message {
    /** */
    public static final short TYPE_CODE = 186;

    /** Min value in column. */
    private StatisticsDecimalMessage min;

    /** Max value in column. */
    private StatisticsDecimalMessage max;

    /** Number of null values in column. */
    private long nulls;

    /** Number of distinct values in column (except nulls). */
    private long distinct;

    /** Total vals in column. */
    private long total;

    /** Average size, for variable size values (in bytes). */
    private int size;

    /** Raw data. */
    private byte[] rawData;

    /** Version. */
    private long ver;

    /** Created at time, milliseconds. */
    private long createdAt;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public StatisticsColumnData() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param min Min value in column.
     * @param max Max value in column.
     * @param nulls Number of null values in column.
     * @param distinct Total distinct values in column.
     * @param total Total values in column.
     * @param size Average size, for variable size types (in bytes).
     * @param rawData Raw data to make statistics aggregate.
     * @param ver Statistics version.
     * @param createdAt Created at time, milliseconds.
     */
    public StatisticsColumnData(
        StatisticsDecimalMessage min,
        StatisticsDecimalMessage max,
        long nulls,
        long distinct,
        long total,
        int size,
        byte[] rawData,
        long ver,
        long createdAt
    ) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.distinct = distinct;
        this.total = total;
        this.size = size;
        this.rawData = rawData;
        this.ver = ver;
        this.createdAt = createdAt;
    }

    /**
     * @return Min value in column.
     */
    public StatisticsDecimalMessage min() {
        return min;
    }

    /**
     * @return Max value in column.
     */
    public StatisticsDecimalMessage max() {
        return max;
    }

    /**
     * @return Number of null values in column.
     */
    public long nulls() {
        return nulls;
    }

    /**
     * @return Total distinct values in column.
     */
    public long distinct() {
        return distinct;
    }

    /**
     * @return Total values in column.
     */
    public long total() {
        return total;
    }

    /**
     * @return Average size, for variable size types (in bytes).
     */
    public int size() {
        return size;
    }

    /**
     * @return Raw data.
     */
    public byte[] rawData() {
        return rawData;
    }

    /**
     * @return Raw data.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Created at time, milliseconds.
     */
    public long createdAt() {
        return createdAt;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeLong(createdAt))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong(distinct))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage(max))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMessage(min))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong(nulls))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray(rawData))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt(size))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong(total))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeLong(ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                createdAt = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                distinct = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                max = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                min = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                nulls = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                rawData = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                size = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                total = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                ver = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.metric;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.agent.dto.metric.MetricRequest.PROTO_VER_1;
import static org.apache.ignite.agent.dto.metric.MetricType.BOOLEAN;
import static org.apache.ignite.agent.dto.metric.MetricType.DOUBLE;
import static org.apache.ignite.agent.dto.metric.MetricType.HISTOGRAM;
import static org.apache.ignite.agent.dto.metric.MetricType.HIT_RATE;
import static org.apache.ignite.agent.dto.metric.MetricType.INT;
import static org.apache.ignite.agent.dto.metric.MetricType.LONG;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.getShort;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;
import static org.apache.ignite.internal.util.GridUnsafe.putShort;

/**
 * Compact data structure.
 *
 * Message format: header | [schema] | [data]
 *
 * Header:
 *  0 - int - message size in bytes.
 *  4 - short - version of protocol.
 *  6 - long - timestamp.
 * 14 - int - version of schema.
 * 18 - int - offset of schema frame (0xFFFFFFFF if no schema frame in message).
 * 22 - int - size of schema frame in bytes (0 if no schema frame in message).
 * 26 - int - offset of data frame (0xFFFFFFFF if no data frame in message).
 * 30 - int - size of data frame in bytes (0 if no data in message).
 * 34 - UUID - cluster ID (two long values: most significant bits, then least significant bits).
 * 50 - int - size of user tag in bytes (0 if user tag isn't defined).
 * 54 - byte[] - user tag.
 * 54 + user tag size - int - size of consistent ID in bytes.
 * 54 + user tag size + 4 - byte[] - consistent ID ()
 *
 * Schema:
 * 0 - byte - particular metric type.
 * 1 - int - size of particular metric name in bytes.
 * 5 - bytes - particular metric name
 *
 * Data:
 * Accordingly to schema.
 *
 */
public class MetricResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Header size without user tag bytes. */
    static final int BASE_HEADER_SIZE = 54;

    /** Message size field offset. */
    private static final int MSG_SIZE_OFF = 0;

    /** Protocol version field offset. */
    private static final int PROTO_VER_OFF = 4;

    /** Timestamp. */
    private static final int TIMESTAMP_OFF = 6;

    /** Schema version field offset. */
    private static final int SCHEMA_VER_OFF = 14;

    /** Schema offset field offset. */
    private static final int SCHEMA_OFF_OFF = 18;

    /** Schema size field offset. */
    private static final int SCHEMA_SIZE_OFF = 22;

    /** Data offset field offset. */
    private static final int DATA_OFF_OFF = 26;

    /** Data size field offset. */
    private static final int DATA_SIZE_OFF = 30;

    /** Cluster ID field offset. */
    private static final int CLUSTER_ID_OFF = 34;

    /** User tag size field offset. */
    private static final int USER_TAG_SIZE_OFF = 50;

    /** User tag field offset. */
    private static final int USER_TAG_OFF = 54;

    /** Constant for indication of absent some block of data. */
    private static final int NO_OFF = -1;

    /** Size of consistent ID length field. */
    static final int CONSISTENT_ID_LEN_SIZE = Integer.BYTES;

    /** Message body. */
    byte[] body;

    /**
     * Default constructor.
     */
    public MetricResponse() {
        // No-op.
    }

    /**
     * @param schemaVer Schema version.
     * @param ts Timestamp.
     * @param clusterId Cluster ID.
     * @param userTag Cluster tag assigned by user.
     * @param consistentId Node consistent ID.
     * @param schemaSize Schema size in binary format.
     * @param dataSize Data size.
     * @param schemaWriter Schema writer.
     * @param dataWriter Data writer.
     */
    public MetricResponse(
            int schemaVer,
            long ts,
            UUID clusterId,
            @Nullable String userTag,
            String consistentId,
            int schemaSize,
            int dataSize,
            BiConsumer<byte[], Integer> schemaWriter,
            BiConsumer<byte[], Integer> dataWriter
    ) {
        byte[] userTagBytes = null;

        int userTagLen = 0;

        if (userTag != null && !userTag.isEmpty()) {
            userTagBytes = userTag.getBytes(UTF_8);

            userTagLen = userTagBytes.length;
        }

        byte[] consistentIdBytes = consistentId.getBytes(UTF_8);

        int len = BASE_HEADER_SIZE + userTagLen + CONSISTENT_ID_LEN_SIZE + consistentIdBytes.length + schemaSize + dataSize;

        int schemaOff = BASE_HEADER_SIZE + userTagLen + CONSISTENT_ID_LEN_SIZE + consistentIdBytes.length;

        int dataOff = schemaOff + schemaSize;

        body = new byte[len];

        header(schemaVer, ts, clusterId, userTagBytes, consistentIdBytes, schemaOff, schemaSize, dataOff, dataSize);

        if (schemaOff > -1)
            schemaWriter.accept(body, schemaOff);

        if (dataOff > -1)
            dataWriter.accept(body, dataOff);
    }

    /**
     * @return Message size.
     */
    public int size() {
        return getInt(body, BYTE_ARR_OFF + MSG_SIZE_OFF);
    }

    /**
     * @return Protocol version.
     */
    public short protocolVersion() {
        return getShort(body, BYTE_ARR_OFF + PROTO_VER_OFF);
    }

    /**
     * @return Timestamp.
     */
    public long timestamp() {
        return getLong(body, BYTE_ARR_OFF + TIMESTAMP_OFF);
    }

    /**
     * @return Schema version.
     */
    public int schemaVersion() {
        return getInt(body, BYTE_ARR_OFF + SCHEMA_VER_OFF);
    }

    /**
     * @return Cluster ID.
     */
    public UUID clusterId() {
        long mostSigBits = getLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF);

        long leastSigBits = getLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF + Long.BYTES);

        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * @return User tag.
     */
    @Nullable public String userTag() {
        int len = userTagSize();

        if (len == 0)
            return null;

        return new String(body, USER_TAG_OFF, len, UTF_8);
    }

    /**
     * @return User tag size.
     */
    private int userTagSize() {
        return getInt(body, BYTE_ARR_OFF + USER_TAG_SIZE_OFF);
    }

    /**
     * @return Consistent ID.
     */
    public String consistentId() {
        int consistentIdSizeOff = BASE_HEADER_SIZE + userTagSize();

        int len = getInt(body, BYTE_ARR_OFF + consistentIdSizeOff);

        int off = consistentIdSizeOff + Integer.BYTES;

        return new String(body, off, len, UTF_8);
    }

    /**
     * @return Metrics schema for this message.
     */
    @Nullable public MetricSchema schema() {
        int off = schemaOffset();

        if (off == NO_OFF)
            return null;

        return MetricSchema.fromBytes(body, off, schemaSize());
    }

    /**
     * @param schema Schema obtained by {@link #schema()}.
     * @param consumer Visitor for the values from this message.
     */
    public void processData(MetricSchema schema, MetricValueConsumer consumer) {
        int off = dataOffset();

        if (off == NO_OFF)
            return;

        VarIntReader data = new VarIntReader(body);

        data.position(off);

        List<MetricSchemaItem> items = schema.items();

        for (MetricSchemaItem item : items) {
            short idx = item.index();

            MetricRegistrySchema regSchema = schema.registrySchema(idx);

            List<MetricRegistrySchemaItem> regItems = regSchema.items();

            for (MetricRegistrySchemaItem regItem : regItems) {
                String name =  item.prefix() + '.' + regItem.name();

                MetricType type = MetricType.findByType(regItem.metricType().type());

                if (type == LONG)
                    consumer.onLong(name, data.getVarLong());
                else if (type == INT)
                    consumer.onInt(name, data.getVarInt());
                else if (type == HIT_RATE) {
                    long interval = data.getVarLong();

                    long val = data.getVarLong();

                    consumer.onLong(name + '.' + interval, val);
                }
                else if (type == HISTOGRAM) {
                    int pairCnt = data.getVarInt();

                    for (int i = 0; i < pairCnt; i++) {
                        long bound = data.getVarLong();

                        long val = data.getVarLong();

                        consumer.onLong(name + '.' + bound, val);
                    }

                    consumer.onLong(name + ".inf", data.getVarLong());
                }
                else if (type == DOUBLE)
                    consumer.onDouble(name, data.getDouble());
                else if (type == BOOLEAN)
                    consumer.onBoolean(name, data.getBoolean());
                else
                    throw new IllegalStateException("Unknown metric type [metric=" + name + ", type=" + type + ']');
            }
        }
    }

    /**
     * @return Schema offset.
     */
    public int schemaOffset() {
        return getInt(body, BYTE_ARR_OFF + SCHEMA_OFF_OFF);
    }

    /**
     * @return Schema size.
     */
    public int schemaSize() {
        return getInt(body, BYTE_ARR_OFF + SCHEMA_SIZE_OFF);
    }

    /**
     * @return Data offset.
     */
    public int dataOffset() {
        return getInt(body, BYTE_ARR_OFF + DATA_OFF_OFF);
    }

    /**
     * @return Data size.
     */
    public int dataSize() {
        return getInt(body, BYTE_ARR_OFF + DATA_SIZE_OFF);
    }

    /**
     * @return Message body.
     */
    public byte[] body() {
        return body;
    }

    /**
     * Writes message header.
     *
     * @param schemaVer Schema version.
     * @param ts Timestamp.
     * @param clusterId Cluster ID.
     * @param userTagBytes User tags bytes.
     * @param consistentIdBytes Consistent ID bytes.
     * @param schemaOff Schema offset.
     * @param schemaSize Schema size.
     * @param dataOff Data offset.
     * @param dataSize Data size.
     */
    private void header(
            int schemaVer,
            long ts,
            UUID clusterId,
            byte[] userTagBytes,
            byte[] consistentIdBytes,
            int schemaOff,
            int schemaSize,
            int dataOff,
            int dataSize
    ) {
        putInt(body, BYTE_ARR_OFF + MSG_SIZE_OFF, body.length);

        putShort(body, BYTE_ARR_OFF + PROTO_VER_OFF, PROTO_VER_1);

        putLong(body, BYTE_ARR_OFF + TIMESTAMP_OFF, ts);

        putInt(body, BYTE_ARR_OFF + SCHEMA_VER_OFF, schemaVer);

        putInt(body, BYTE_ARR_OFF + SCHEMA_OFF_OFF, schemaOff);

        putInt(body, BYTE_ARR_OFF + SCHEMA_SIZE_OFF, schemaSize);

        putInt(body, BYTE_ARR_OFF + DATA_OFF_OFF, dataOff);

        putInt(body, BYTE_ARR_OFF + DATA_SIZE_OFF, dataSize);

        putLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF, clusterId.getMostSignificantBits());

        putLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF + Long.BYTES, clusterId.getLeastSignificantBits());

        int off = BASE_HEADER_SIZE;

        if (userTagBytes != null) {
            putInt(body, BYTE_ARR_OFF + USER_TAG_SIZE_OFF, userTagBytes.length);

            copyMemory(userTagBytes, BYTE_ARR_OFF, body, BYTE_ARR_OFF + off, userTagBytes.length);

            off += userTagBytes.length;
        }
        else
            putInt(body, BYTE_ARR_OFF + USER_TAG_SIZE_OFF, 0);

        putInt(body, BYTE_ARR_OFF + off, consistentIdBytes.length);

        off += Integer.BYTES;

        copyMemory(consistentIdBytes, BYTE_ARR_OFF, body, BYTE_ARR_OFF + off, consistentIdBytes.length);
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
                if (!writer.writeByteArray("body", body))
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
                body = reader.readByteArray("body");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(MetricResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
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
        return S.toString(MetricResponse.class, this,
            "protoVer", protocolVersion(),
            "timestamp", timestamp(),
            "consistentId", consistentId(),
            "schemaSize", schemaSize(),
            "dataSize", dataSize()
        );
    }
}

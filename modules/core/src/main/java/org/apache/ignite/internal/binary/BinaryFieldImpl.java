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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOffheapInputStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryField;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.*;

/**
 * Implementation of binary field descriptor.
 */
public class BinaryFieldImpl implements BinaryFieldEx {
    /** Binary context that created this field. */
    private final BinaryContext ctx;

    /** Type ID. */
    private final int typeId;

    /** Well-known object schemas. */
    @GridToStringExclude
    private final BinarySchemaRegistry schemas;

    /** Field name. */
    private final String fieldName;

    /** Pre-calculated field ID. */
    private final int fieldId;

    /**
     * Constructor.
     *
     * @param schemas Schemas.
     * @param fieldName Field name.
     * @param fieldId Field ID.
     */
    public BinaryFieldImpl(
        BinaryContext ctx,
        int typeId,
        BinarySchemaRegistry schemas,
        String fieldName,
        int fieldId
    ) {
        assert ctx != null;
        assert typeId != 0;
        assert schemas != null;
        assert fieldName != null;
        assert fieldId != 0;

        this.ctx = ctx;
        this.typeId = typeId;
        this.schemas = schemas;
        this.fieldName = fieldName;
        this.fieldId = fieldId;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return fieldName;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(BinaryObject obj) {
        BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

        return fieldOrder(obj0) != BinarySchema.ORDER_NOT_FOUND;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T value(BinaryObject obj) {
        BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

        int order = fieldOrder(obj0);

        return order != BinarySchema.ORDER_NOT_FOUND ? (T)obj0.fieldByOrder(order) : null;
    }

    /** {@inheritDoc} */
    @Override public int fieldOffset(byte[] valBytes, int off) {
        int typeId = BinaryPrimitives.readInt(valBytes, off + TYPE_ID_POS);

        if (typeId != this.typeId) {
            throw new BinaryObjectException("Failed to get field because type ID of passed object differs" +
                " from type ID this " + BinaryField.class.getSimpleName() + " belongs to [expected=" + this.typeId +
                ", actual=" + typeId + ']');
        }

        int schemaId = BinaryPrimitives.readInt(valBytes, off + SCHEMA_ID_POS);

        BinarySchema schema = schemas.schema(schemaId);

        if (schema == null) {
            BinaryHeapInputStream in = BinaryHeapInputStream.create(valBytes, off);

            BinaryObjectExImpl obj = (BinaryObjectExImpl)BinaryUtils.unmarshal(in, ctx, null);

            assert obj != null;

            schema = obj.createSchema();
        }

        assert schema != null;

        int order = schema.order(fieldId);

        if (order == BinarySchema.ORDER_NOT_FOUND)
            return -1;

        int schemaOff = BinaryPrimitives.readInt(valBytes, off + SCHEMA_OR_RAW_OFF_POS);

        short flags = BinaryPrimitives.readShort(valBytes, off + FLAGS_POS);

        int fieldIdLen = BinaryUtils.isCompactFooter(flags) ? 0 : BinaryUtils.FIELD_ID_LEN;
        int fieldOffLen = BinaryUtils.fieldOffsetLength(flags);

        int fieldOffPos = schemaOff + order * (fieldIdLen + fieldOffLen) + fieldIdLen;

        int fieldOff;

        if (fieldOffLen == BinaryUtils.OFFSET_1)
            fieldOff = ((int)BinaryPrimitives.readByte(valBytes, off + fieldOffPos) & 0xFF);
        else if (fieldOffLen == BinaryUtils.OFFSET_2)
            fieldOff = ((int)BinaryPrimitives.readShort(valBytes, off + fieldOffPos) & 0xFFFF);
        else
            fieldOff = BinaryPrimitives.readInt(valBytes, off + fieldOffPos);

        return fieldOff;
    }

    /** {@inheritDoc} */
    @Override public int fieldOffset(long addr, int len) {
        int typeId = BinaryPrimitives.readInt(addr, TYPE_ID_POS);

        if (typeId != this.typeId) {
            throw new BinaryObjectException("Failed to get field because type ID of passed object differs" +
                " from type ID this " + BinaryField.class.getSimpleName() + " belongs to [expected=" + this.typeId +
                ", actual=" + typeId + ']');
        }

        int schemaId = BinaryPrimitives.readInt(addr, SCHEMA_ID_POS);

        BinarySchema schema = schemas.schema(schemaId);

        if (schema == null) {
            BinaryOffheapInputStream in = new BinaryOffheapInputStream(addr, len);

            BinaryObjectExImpl obj = (BinaryObjectExImpl)BinaryUtils.unmarshal(in, ctx, null);

            assert obj != null;

            schema = obj.createSchema();
        }

        assert schema != null;

        int order = schema.order(fieldId);

        if (order == BinarySchema.ORDER_NOT_FOUND)
            return -1;

        int schemaOff = BinaryPrimitives.readInt(addr, SCHEMA_OR_RAW_OFF_POS);

        short flags = BinaryPrimitives.readShort(addr, FLAGS_POS);

        int fieldIdLen = BinaryUtils.isCompactFooter(flags) ? 0 : BinaryUtils.FIELD_ID_LEN;
        int fieldOffLen = BinaryUtils.fieldOffsetLength(flags);

        int fieldOffPos = schemaOff + order * (fieldIdLen + fieldOffLen) + fieldIdLen;

        int fieldOff;

        if (fieldOffLen == BinaryUtils.OFFSET_1)
            fieldOff = ((int)BinaryPrimitives.readByte(addr, fieldOffPos) & 0xFF);
        else if (fieldOffLen == BinaryUtils.OFFSET_2)
            fieldOff = ((int)BinaryPrimitives.readShort(addr, fieldOffPos) & 0xFFFF);
        else
            fieldOff = BinaryPrimitives.readInt(addr, fieldOffPos);

        return fieldOff;
    }

    /** {@inheritDoc} */
    @Override public boolean writeField(BinaryObject obj, ByteBuffer buf) {
        BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

        int order = fieldOrder(obj0);

        return obj0.writeFieldByOrder(order, buf);
    }

    /** {@inheritDoc} */
    @Override public <F> F readField(ByteBuffer buf) {
        ByteOrder oldOrder = buf.order();

        try {
            buf.order(ByteOrder.LITTLE_ENDIAN);

            int pos = buf.position();

            byte hdr = buf.get();

            Object val;

            switch (hdr) {
                case INT:
                    val = buf.getInt();

                    break;

                case LONG:
                    val = buf.getLong();

                    break;

                case BOOLEAN:
                    val = buf.get() != 0;

                    break;

                case SHORT:
                    val = buf.getShort();

                    break;

                case BYTE:
                    val = buf.get();

                    break;

                case CHAR:
                    val = buf.getChar();

                    break;

                case FLOAT:
                    val = buf.getFloat();

                    break;

                case DOUBLE:
                    val = buf.getDouble();

                    break;

                case STRING: {
                    int dataLen = buf.getInt();

                    byte[] data = new byte[dataLen];

                    buf.get(data);

                    val = new String(data, 0, dataLen, UTF_8);

                    break;
                }

                case DATE: {
                    long time = buf.getLong();

                    val = new Date(time);

                    break;
                }

                case TIMESTAMP: {
                    long time = buf.getLong();
                    int nanos = buf.getInt();

                    Timestamp ts = new Timestamp(time);

                    ts.setNanos(ts.getNanos() + nanos);

                    val = ts;

                    break;
                }

                case UUID: {
                    long most = buf.getLong();
                    long least = buf.getLong();

                    val = new UUID(most, least);

                    break;
                }

                case DECIMAL: {
                    int scale = buf.getInt();

                    int dataLen = buf.getInt();

                    byte[] data = new byte[dataLen];

                    buf.get(data);

                    BigInteger intVal = new BigInteger(data);

                    if (scale < 0) {
                        scale &= 0x7FFFFFFF;

                        intVal = intVal.negate();
                    }

                    val = new BigDecimal(intVal, scale);

                    break;
                }

                case NULL:
                    val = null;

                    break;

                default:
                    // Restore buffer position.
                    buf.position(pos);

                    val = BinaryUtils.unmarshal(BinaryByteBufferInputStream.create(buf), ctx, null);

                    break;
            }

            return (F)val;
        }
        finally {
            buf.order(oldOrder);
        }
    }

    /**
     * Get relative field offset.
     *
     * @param obj Object.
     * @return Field offset.
     */
    private int fieldOrder(BinaryObjectExImpl obj) {
        if (typeId != obj.typeId()) {
            throw new BinaryObjectException("Failed to get field because type ID of passed object differs" +
                " from type ID this " + BinaryField.class.getSimpleName() + " belongs to [expected=" + typeId +
                ", actual=" + obj.typeId() + ']');
        }

        int schemaId = obj.schemaId();

        BinarySchema schema = schemas.schema(schemaId);

        if (schema == null) {
            schema = obj.createSchema();

            schemas.addSchema(schemaId, schema);
        }

        assert schema != null;

        return schema.order(fieldId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldImpl.class, this);
    }
}

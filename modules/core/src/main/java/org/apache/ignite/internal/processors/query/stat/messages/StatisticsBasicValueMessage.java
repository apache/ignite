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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.BooleanIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ByteIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.BytesIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DateIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DecimalIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DoubleIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.FloatIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IntegerIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.JavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.LongIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.PlainJavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ShortIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.StringIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimeIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimestampIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.UuidIndexKey;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
@IgniteCodeGeneratingFail
public class StatisticsBasicValueMessage implements StatisticsValueMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int BYTE_ARRAY_OFFSET = 4;

    /** */
    public static final short TYPE_CODE = 184;

    /** */
    private int type;

    /** */
    private byte[] data;

    /** */
    private IndexKey val;

    /** */
    public StatisticsBasicValueMessage() {
        // No-op.
    }

    /** */
    public StatisticsBasicValueMessage(IndexKey val) {
        this.val = val;
        type = val.type().code();
    }

    /**
     * Gets H2 value.
     *
     * @param ctx Kernal context.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Override public IndexKey value(GridKernalContext ctx) throws IgniteCheckedException {
        if (val == null)
            deserializeData(type, data);
       
        assert val != null;
        
        return val;
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
                if (!writer.writeInt("type", type))
                    return false;

                writer.incrementState();

            case 1:
                if (data == null)
                    serializeData();

                if (!writer.writeByteArray("data", data))
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
                type = reader.readInt("data");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                data = reader.readByteArray("data");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatisticsBasicValueMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
    
    /** */
    private void serializeData() {
        if (data != null)
            return;
        
        switch (IndexKeyType.forCode(type)) {
            case NULL:
                data = null;
                break;

            case BOOLEAN: {
                data = new byte[1];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putInt(type);
                buf.put((Boolean)val.key() ? (byte)1 : (byte)0);
                break;
            }
            case BYTE: {
                data = new byte[1];
                ByteBuffer buf = ByteBuffer.wrap(data);
                buf.put((Byte)val.key());
                break;
            }
            case SHORT: {
                data = new byte[2];
                ByteBuffer buf = ByteBuffer.wrap(data);
                buf.putShort((Short)val.key());
                break;
            }
            case INT: {
                data = new byte[4];
                ByteBuffer buf = ByteBuffer.wrap(data);
                buf.putInt((Integer)val.key());
                break;
            }
            case LONG: {
                data = new byte[8];
                ByteBuffer buf = ByteBuffer.wrap(data);
                buf.putLong((Long)val.key());
                break;
            }
            case DECIMAL: {
                byte[] decData = ((BigDecimal)val.key()).unscaledValue().toByteArray();
                int scale = ((BigDecimal)val.key()).scale();

                data = new byte[BYTE_ARRAY_OFFSET + decData.length + 4];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putInt(decData.length);
                buf.put(decData);
                buf.putInt(scale);
                break;
            }

            case FLOAT: {
                data = new byte[4];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putFloat((Float)val.key());
                break;
            }
            case DOUBLE: {
                data = new byte[8];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putDouble((Double)val.key());
                break;
            }
            case TIME: {
                data = new byte[8];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putLong(((TimeIndexKey)val.key()).nanos());
                break;
            }
            case DATE: {
                data = new byte[8];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putLong(((DateIndexKey)val.key()).dateValue());
                break;
            }
            case TIMESTAMP: {
                data = new byte[16];
                ByteBuffer buf = ByteBuffer.wrap(data);

                TimestampIndexKey key = (TimestampIndexKey)val.key();
                buf.putLong(key.dateValue());
                buf.putLong(key.nanos());
                break;
            }
            case BYTES: {
                int size = ((byte[])val.key()).length;
                data = new byte[BYTE_ARRAY_OFFSET + size];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putInt(size);
                buf.put((byte[])val.key());
                break;
            }

            case STRING: {
                byte[] strData = ((String)val.key()).getBytes(StandardCharsets.UTF_8);
                int size = strData.length;
                
                data = new byte[BYTE_ARRAY_OFFSET + size];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putInt(size);
                buf.put(strData);
                break;
            }
            case UUID: {
                data = new byte[16];
                ByteBuffer buf = ByteBuffer.wrap(data);

                UUID key = (UUID)val.key();
                buf.putLong(key.getMostSignificantBits());
                buf.putLong(key.getLeastSignificantBits());
                break;
            }
            case JAVA_OBJECT:
                byte[] objData = ((JavaObjectIndexKey)val.key()).bytesNoCopy();
                int size = objData.length;

                data = new byte[BYTE_ARRAY_OFFSET + size];
                ByteBuffer buf = ByteBuffer.wrap(data);

                buf.putInt(objData.length);
                buf.put(objData);
                break;
            default:
                throw new IgniteException("Unsupported IndexKey type: " + IndexKeyType.forCode(type));
        }
    }

    /** */
    private void deserializeData(int type, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        switch (IndexKeyType.forCode(type)) {
            case NULL:
                val = NullIndexKey.INSTANCE;
                break;

            case BOOLEAN:
                val = buf.get() == 1 ? new BooleanIndexKey(true) : new BooleanIndexKey(false);
                break;

            case BYTE:
                val = new ByteIndexKey(buf.get());
                break;

            case SHORT:
                val = new ShortIndexKey(buf.getShort());
                break;

            case INT:
                val = new IntegerIndexKey(buf.getInt());
                break;

            case LONG:
                val = new LongIndexKey(buf.getLong());
                break;

            case DECIMAL: {
                byte[] b = new byte[buf.getInt()];
                buf.get(b);

                val = new DecimalIndexKey(new BigDecimal(new BigInteger(b), buf.getInt()));
                break;
            }

            case FLOAT:
                val = new FloatIndexKey(buf.getFloat());
                break;

            case DOUBLE:
                val = new DoubleIndexKey(buf.getDouble());
                break;

            case TIME:
                val = new TimeIndexKey(buf.getLong());
                break;

            case DATE:
                val = new DateIndexKey(buf.getLong());
                break;

            case TIMESTAMP:
                val = new TimestampIndexKey(buf.getLong(), buf.getLong());
                break;

            case BYTES: {
                byte[] b = new byte[buf.getInt()];
                buf.get(b);
                val = new BytesIndexKey(b);
                break;
            }

            case STRING: {
                byte[] b = new byte[buf.getInt()];
                buf.get(b);
                val = new StringIndexKey(new String(b, StandardCharsets.UTF_8));
                break;
            }

            case UUID:
                val = new UuidIndexKey(new UUID(buf.getLong(), buf.getLong()));
                break;

            case JAVA_OBJECT:
                byte[] b = new byte[buf.getInt()];
                buf.get(b);
                val = new PlainJavaObjectIndexKey(null, b);
                
                break;
            default:
                throw new IgniteException("Unsupported IndexKey type: " + IndexKeyType.forCode(type));
        }
    }
}

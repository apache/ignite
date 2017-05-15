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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractSqlBinaryReader extends BinaryReaderExImpl {
    /**
     * @param ctx Binary context.
     * @param in Binary input stream.
     * @param ldr Calss loader.
     * @param forUnmarshal Use to unmarshal flag.
     */
    public AbstractSqlBinaryReader(BinaryContext ctx,
        BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal) {
        super(ctx, in, ldr, forUnmarshal);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObjectDetached() throws BinaryObjectException {
        byte type = readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.BOOLEAN:
                return readBoolean();

            case GridBinaryMarshaller.BYTE:
                return readByte();

            case GridBinaryMarshaller.CHAR:
                return readChar();

            case GridBinaryMarshaller.SHORT:
                return readShort();

            case GridBinaryMarshaller.INT:
                return readInt();

            case GridBinaryMarshaller.LONG:
                return readLong();

            case GridBinaryMarshaller.FLOAT:
                return readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return readDouble();

            case GridBinaryMarshaller.STRING:
                return BinaryUtils.doReadString(in());

            case GridBinaryMarshaller.DECIMAL:
                return BinaryUtils.doReadDecimal(in());

            case GridBinaryMarshaller.UUID:
                return BinaryUtils.doReadUuid(in());

            case GridBinaryMarshaller.TIME:
                return BinaryUtils.doReadTime(in());

            case GridBinaryMarshaller.TIMESTAMP:
                return BinaryUtils.doReadTimestamp(in());

            case GridBinaryMarshaller.DATE:
                return BinaryUtils.doReadSqlDate(in());

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return BinaryUtils.doReadBooleanArray(in());

            case GridBinaryMarshaller.BYTE_ARR:
                return BinaryUtils.doReadByteArray(in());

            case GridBinaryMarshaller.CHAR_ARR:
                return BinaryUtils.doReadCharArray(in());

            case GridBinaryMarshaller.SHORT_ARR:
                return BinaryUtils.doReadShortArray(in());

            case GridBinaryMarshaller.INT_ARR:
                return BinaryUtils.doReadIntArray(in());

            case GridBinaryMarshaller.FLOAT_ARR:
                return BinaryUtils.doReadFloatArray(in());

            case GridBinaryMarshaller.DOUBLE_ARR:
                return BinaryUtils.doReadDoubleArray(in());

            case GridBinaryMarshaller.STRING_ARR:
                return BinaryUtils.doReadStringArray(in());

            case GridBinaryMarshaller.DECIMAL_ARR:
                return BinaryUtils.doReadDecimalArray(in());

            case GridBinaryMarshaller.UUID_ARR:
                return BinaryUtils.doReadUuidArray(in());

            case GridBinaryMarshaller.TIME_ARR:
                return BinaryUtils.doReadTimeArray(in());

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return BinaryUtils.doReadTimestampArray(in());

            case GridBinaryMarshaller.DATE_ARR:
                return BinaryUtils.doReadSqlDateArray(in());

            case GridBinaryMarshaller.JDK_MARSH:
                return readNotEmbeddedObject();

            default:
                throw new BinaryObjectException("Unknown object type: " + type);
        }
    }

    /**
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    protected Object binReadObjectDetached() throws BinaryObjectException {
        return super.readObjectDetached();
    }

    /**
     * @return An object is unmarshaled by marshaller.
     * @throws BinaryObjectException On error.
     */
    protected abstract Object readNotEmbeddedObject() throws BinaryObjectException;
}

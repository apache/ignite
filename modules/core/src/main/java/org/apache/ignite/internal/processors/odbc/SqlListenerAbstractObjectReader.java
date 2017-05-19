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
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
@SuppressWarnings("unchecked")
public abstract class SqlListenerAbstractObjectReader {
    /**
     * @param reader Reader.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public Object readObject(BinaryReaderExImpl reader) throws BinaryObjectException {
        byte type = reader.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.BOOLEAN:
                return reader.readBoolean();

            case GridBinaryMarshaller.BYTE:
                return reader.readByte();

            case GridBinaryMarshaller.CHAR:
                return reader.readChar();

            case GridBinaryMarshaller.SHORT:
                return reader.readShort();

            case GridBinaryMarshaller.INT:
                return reader.readInt();

            case GridBinaryMarshaller.LONG:
                return reader.readLong();

            case GridBinaryMarshaller.FLOAT:
                return reader.readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return reader.readDouble();

            case GridBinaryMarshaller.STRING:
                return BinaryUtils.doReadString(reader.in());

            case GridBinaryMarshaller.DECIMAL:
                return BinaryUtils.doReadDecimal(reader.in());

            case GridBinaryMarshaller.UUID:
                return BinaryUtils.doReadUuid(reader.in());

            case GridBinaryMarshaller.TIME:
                return BinaryUtils.doReadTime(reader.in());

            case GridBinaryMarshaller.TIMESTAMP:
                return BinaryUtils.doReadTimestamp(reader.in());

            case GridBinaryMarshaller.DATE:
                return BinaryUtils.doReadDate(reader.in());

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return BinaryUtils.doReadBooleanArray(reader.in());

            case GridBinaryMarshaller.BYTE_ARR:
                return BinaryUtils.doReadByteArray(reader.in());

            case GridBinaryMarshaller.CHAR_ARR:
                return BinaryUtils.doReadCharArray(reader.in());

            case GridBinaryMarshaller.SHORT_ARR:
                return BinaryUtils.doReadShortArray(reader.in());

            case GridBinaryMarshaller.INT_ARR:
                return BinaryUtils.doReadIntArray(reader.in());

            case GridBinaryMarshaller.FLOAT_ARR:
                return BinaryUtils.doReadFloatArray(reader.in());

            case GridBinaryMarshaller.DOUBLE_ARR:
                return BinaryUtils.doReadDoubleArray(reader.in());

            case GridBinaryMarshaller.STRING_ARR:
                return BinaryUtils.doReadStringArray(reader.in());

            case GridBinaryMarshaller.DECIMAL_ARR:
                return BinaryUtils.doReadDecimalArray(reader.in());

            case GridBinaryMarshaller.UUID_ARR:
                return BinaryUtils.doReadUuidArray(reader.in());

            case GridBinaryMarshaller.TIME_ARR:
                return BinaryUtils.doReadTimeArray(reader.in());

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return BinaryUtils.doReadTimestampArray(reader.in());

            case GridBinaryMarshaller.DATE_ARR:
                return BinaryUtils.doReadDateArray(reader.in());

            default:
                reader.in().position(reader.in().position() - 1);

                return readCustomObject(reader);
        }
    }

    /**
     * @param reader Reader.
     * @return An object is unmarshaled by marshaller.
     * @throws BinaryObjectException On error.
     */
    protected abstract Object readCustomObject(BinaryReaderExImpl reader) throws BinaryObjectException;
}

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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
@SuppressWarnings("unchecked")
public class BinaryReaderWithJdkObjectImpl extends BinaryReaderExImpl {
    /** Jdk marshaller. */
    private JdkMarshaller jdkMars = new JdkMarshaller();
    /**
     * @param in Binary reader.
     */
    public BinaryReaderWithJdkObjectImpl(BinaryInputStream in) {
        super(null, in, null, false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObjectDetached() throws BinaryObjectException {
        byte type = readByte();

        if (type == GridBinaryMarshaller.NULL)
            return null;
        else if (type == GridBinaryMarshaller.BOOLEAN)
            return readBoolean();
        else if (type == GridBinaryMarshaller.BYTE)
            return readByte();
        else if (type == GridBinaryMarshaller.CHAR)
            return readChar();
        else if (type == GridBinaryMarshaller.SHORT)
            return readShort();
        else if (type == GridBinaryMarshaller.INT)
            return readInt();
        else if (type == GridBinaryMarshaller.LONG)
            return readLong();
        else if (type == GridBinaryMarshaller.FLOAT)
            return readFloat();
        else if (type == GridBinaryMarshaller.DOUBLE)
            return readDouble();
        else if (type == GridBinaryMarshaller.STRING)
            return BinaryUtils.doReadString(in());
        else if (type == GridBinaryMarshaller.DECIMAL)
            return BinaryUtils.doReadDecimal(in());
        else if (type == GridBinaryMarshaller.UUID)
            return BinaryUtils.doReadUuid(in());
        else if (type == GridBinaryMarshaller.TIME)
            return BinaryUtils.doReadTime(in());
        else if (type == GridBinaryMarshaller.TIMESTAMP)
            return BinaryUtils.doReadTimestamp(in());
        else if (type == GridBinaryMarshaller.DATE)
            return BinaryUtils.doReadDate(in());
        else if (type == GridBinaryMarshaller.BOOLEAN_ARR)
            return BinaryUtils.doReadBooleanArray(in());
        else if (type == GridBinaryMarshaller.BYTE_ARR)
            return BinaryUtils.doReadByteArray(in());
        else if (type == GridBinaryMarshaller.CHAR_ARR)
            return BinaryUtils.doReadCharArray(in());
        else if (type == GridBinaryMarshaller.SHORT_ARR)
            return BinaryUtils.doReadShortArray(in());
        else if (type == GridBinaryMarshaller.INT_ARR)
            return BinaryUtils.doReadIntArray(in());
        else if (type == GridBinaryMarshaller.FLOAT_ARR)
            return BinaryUtils.doReadFloatArray(in());
        else if (type == GridBinaryMarshaller.DOUBLE_ARR)
            return BinaryUtils.doReadDoubleArray(in());
        else if (type == GridBinaryMarshaller.STRING_ARR)
            return BinaryUtils.doReadStringArray(in());
        else if (type == GridBinaryMarshaller.DECIMAL_ARR)
            return BinaryUtils.doReadDecimalArray(in());
        else if (type == GridBinaryMarshaller.UUID_ARR)
            return BinaryUtils.doReadUuidArray(in());
        else if (type == GridBinaryMarshaller.TIME_ARR)
            return BinaryUtils.doReadTimeArray(in());
        else if (type == GridBinaryMarshaller.TIMESTAMP_ARR)
            return BinaryUtils.doReadTimestampArray(in());
        else if (type == GridBinaryMarshaller.DATE_ARR)
            return BinaryUtils.doReadDateArray(in());
        else
            return readJdkObject();
    }

    /**
     * @return An object is unmarshaled by JDK marshaller.
     */
    private Object readJdkObject() {
        try {
            return U.unmarshal(jdkMars, BinaryUtils.doReadByteArray(in()), null);
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException(e);
        }
    }
}

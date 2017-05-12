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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Binary writer with marshaling non-primitive and non-embedded objects with JDK marshaller..
 */
public class BinaryWriterWithJdkObjectImpl extends BinaryWriterExImpl {
    /** Jdk marshaller. */
    private JdkMarshaller jdkMars = new JdkMarshaller();

    /**
     * @param out Binary writer.
     */
    public BinaryWriterWithJdkObjectImpl(BinaryOutputStream out) {
        super(null, out, null, null);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectDetached(@Nullable Object obj) throws BinaryObjectException {
        Class<?> cls = obj.getClass();

        if (cls == Boolean.class)
            doWriteBoolean((Boolean)obj);
        else if (cls == Byte.class)
            doWriteByte((Byte)obj);
        else if (cls == Character.class)
            doWriteChar((Character)obj);
        else if (cls == Short.class)
            doWriteShort((Short)obj);
        else if (cls == Integer.class)
            doWriteInt((Integer)obj);
        else if (cls == Long.class)
            doWriteLong((Long)obj);
        else if (cls == Float.class)
            doWriteFloat((Float)obj);
        else if (cls == Double.class)
            doWriteDouble((Double)obj);
        else if (cls == String.class)
            doWriteString((String)obj);
        else if (cls == BigDecimal.class)
            doWriteDecimal((BigDecimal)obj);
        else if (cls == UUID.class)
            writeUuid((UUID)obj);
        else if (cls == Time.class)
            writeTime((Time)obj);
        else if (cls == Timestamp.class)
            writeTimestamp((Timestamp)obj);
        else if (cls == Date.class)
            writeDate((Date)obj);
        else if (cls == boolean[].class)
            writeBooleanArray((boolean[])obj);
        else if (cls == byte[].class)
            writeByteArray((byte[])obj);
        else if (cls == char[].class)
            writeCharArray((char[])obj);
        else if (cls == short[].class)
            writeShortArray((short[])obj);
        else if (cls == int[].class)
            writeIntArray((int[])obj);
        else if (cls == float[].class)
            writeFloatArray((float[])obj);
        else if (cls == double[].class)
            writeDoubleArray((double[])obj);
        else if (cls == String[].class)
            writeStringArray((String[])obj);
        else if (cls == BigDecimal[].class)
            writeDecimalArray((BigDecimal[])obj);
        else if (cls == UUID[].class)
            writeUuidArray((UUID[])obj);
        else if (cls == Time[].class)
            writeTimeArray((Time[])obj);
        else if (cls == Timestamp[].class)
            writeTimestampArray((Timestamp[])obj);
        else if (cls == Date[].class)
            writeDateArray((Date[])obj);
        else
            writeJdkObject(obj);
    }

    /**
     * @param obj Object to marshal with JDK marshaller and write to binary stream.
     */
    private void writeJdkObject(Object obj) {
        writeByte(GridBinaryMarshaller.JDK_MARSH);

        try {
            writeByteArray(U.marshal(jdkMars, obj));
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException(e);
        }
    }
}

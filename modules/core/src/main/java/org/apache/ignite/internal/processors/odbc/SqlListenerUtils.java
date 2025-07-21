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

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.jdbc2.JdbcBinaryBuffer;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
public abstract class SqlListenerUtils {
    /**
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(BinaryReaderEx reader, boolean binObjAllow)
        throws BinaryObjectException {
        return readObject(reader, binObjAllow, true);
    }

    /**
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @param keepBinary Whether to deserialize objects or keep in binary format.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(BinaryReaderEx reader, boolean binObjAllow, boolean keepBinary)
        throws BinaryObjectException {
        return readObject(reader, binObjAllow, keepBinary, true);
    }

    /**
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @param keepBinary Whether to deserialize objects or keep in binary format.
     * @param createByteArrayCopy Whether to return new copy or copy-on-write buffer for byte array.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(BinaryReaderEx reader, boolean binObjAllow,
        boolean keepBinary, boolean createByteArrayCopy) throws BinaryObjectException {
        byte type = reader.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
            case GridBinaryMarshaller.BOOLEAN:
            case GridBinaryMarshaller.BYTE:
            case GridBinaryMarshaller.CHAR:
            case GridBinaryMarshaller.SHORT:
            case GridBinaryMarshaller.INT:
            case GridBinaryMarshaller.LONG:
            case GridBinaryMarshaller.FLOAT:
            case GridBinaryMarshaller.DOUBLE:
            case GridBinaryMarshaller.STRING:
            case GridBinaryMarshaller.DECIMAL:
            case GridBinaryMarshaller.UUID:
            case GridBinaryMarshaller.DATE:
            case GridBinaryMarshaller.TIMESTAMP:
            case GridBinaryMarshaller.TIME:
            case GridBinaryMarshaller.BOOLEAN_ARR:
            case GridBinaryMarshaller.CHAR_ARR:
            case GridBinaryMarshaller.SHORT_ARR:
            case GridBinaryMarshaller.INT_ARR:
            case GridBinaryMarshaller.LONG_ARR:
            case GridBinaryMarshaller.FLOAT_ARR:
            case GridBinaryMarshaller.DOUBLE_ARR:
            case GridBinaryMarshaller.STRING_ARR:
            case GridBinaryMarshaller.DECIMAL_ARR:
            case GridBinaryMarshaller.UUID_ARR:
            case GridBinaryMarshaller.TIME_ARR:
            case GridBinaryMarshaller.TIMESTAMP_ARR:
            case GridBinaryMarshaller.DATE_ARR:
                BinaryUtils.unmarshallCommon(reader.in(), type);

            case GridBinaryMarshaller.BYTE_ARR:
                return readByteArray(reader, createByteArrayCopy);


            default:
                reader.in().position(reader.in().position() - 1);

                if (binObjAllow) {
                    Object res = reader.readObjectDetached();

                    return !keepBinary && res instanceof BinaryObject
                        ? ((BinaryObject)res).deserialize()
                        : res;
                }
                else
                    throw new BinaryObjectException("Custom objects are not supported");
        }
    }

    /**
     * Read byte array using the reader.
     *
     * <p>Returns either (eagerly) new instance of the byte array with all data materialized,
     * or {@link JdbcBinaryBuffer} which wraps part of the array enclosed in
     * the reader's input stream in a copy-on-write manner.
     *
     * @param reader Reader.
     * @param createByteArrayCopy Whether create new byte array copy or try to create copy-on-write buffer.
     * @return Either byte[] or {@link JdbcBinaryBuffer}.
     */
    private static Object readByteArray(BinaryReaderEx reader, boolean createByteArrayCopy) {
        if (!createByteArrayCopy && reader.in().hasArray()) {
            int len = reader.in().readInt();

            int position = reader.in().position();

            reader.in().position(position + len);

            return JdbcBinaryBuffer.createReadOnly(reader.in().array(), position, len);
        }
        else
            return BinaryUtils.doReadByteArray(reader.in());
    }

    /**
     * @param writer Writer.
     * @param obj Object to write.
     * @param binObjAllow Allow to write non plain objects.
     * @throws BinaryObjectException On error.
     */
    public static void writeObject(BinaryWriterEx writer, @Nullable Object obj, boolean binObjAllow)
        throws BinaryObjectException {
        if (obj == null) {
            writer.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        Class<?> cls = obj.getClass();

        if (cls == Boolean.class)
            writer.writeBooleanFieldPrimitive((Boolean)obj);
        else if (cls == Byte.class)
            writer.writeByteFieldPrimitive((Byte)obj);
        else if (cls == Character.class)
            writer.writeCharFieldPrimitive((Character)obj);
        else if (cls == Short.class)
            writer.writeShortFieldPrimitive((Short)obj);
        else if (cls == Integer.class)
            writer.writeIntFieldPrimitive((Integer)obj);
        else if (cls == Long.class)
            writer.writeLongFieldPrimitive((Long)obj);
        else if (cls == Float.class)
            writer.writeFloatFieldPrimitive((Float)obj);
        else if (cls == Double.class)
            writer.writeDoubleFieldPrimitive((Double)obj);
        else if (cls == String.class)
            writer.writeString((String)obj);
        else if (cls == BigDecimal.class)
            writer.writeDecimal((BigDecimal)obj);
        else if (cls == UUID.class)
            writer.writeUuid((UUID)obj);
        else if (cls == Time.class)
            writer.writeTime((Time)obj);
        else if (cls == Timestamp.class)
            writer.writeTimestamp((Timestamp)obj);
        else if (cls == java.sql.Date.class || cls == java.util.Date.class)
            writer.writeDate((java.util.Date)obj);
        else if (cls == boolean[].class)
            writer.writeBooleanArray((boolean[])obj);
        else if (cls == byte[].class)
            writer.writeByteArray((byte[])obj);
        else if (cls == char[].class)
            writer.writeCharArray((char[])obj);
        else if (cls == short[].class)
            writer.writeShortArray((short[])obj);
        else if (cls == int[].class)
            writer.writeIntArray((int[])obj);
        else if (cls == long[].class)
            writer.writeLongArray((long[])obj);
        else if (cls == float[].class)
            writer.writeFloatArray((float[])obj);
        else if (cls == double[].class)
            writer.writeDoubleArray((double[])obj);
        else if (cls == String[].class)
            writer.writeStringArray((String[])obj);
        else if (cls == BigDecimal[].class)
            writer.writeDecimalArray((BigDecimal[])obj);
        else if (cls == UUID[].class)
            writer.writeUuidArray((UUID[])obj);
        else if (cls == Time[].class)
            writer.writeTimeArray((Time[])obj);
        else if (cls == Timestamp[].class)
            writer.writeTimestampArray((Timestamp[])obj);
        else if (cls == java.util.Date[].class || cls == java.sql.Date[].class)
            writer.writeDateArray((java.util.Date[])obj);
        else if (obj instanceof SqlInputStreamWrapper)
            writeByteArray(writer, (SqlInputStreamWrapper)obj);
        else if (obj instanceof Blob)
            writeByteArray(writer, (Blob)obj);
        else if (binObjAllow)
            writer.writeObjectDetached(obj);
        else
            throw new BinaryObjectException("Custom objects are not supported");
    }

    /**
     * Write byte array from the InputStream enclosed in the stream wrapper.
     *
     * @param writer Writer.
     * @param wrapper stream wrapper
     */
    private static void writeByteArray(BinaryWriterEx writer, SqlInputStreamWrapper wrapper) throws BinaryObjectException {
        int written = writer.writeByteArray(wrapper.inputStream(), wrapper.length());

        if (wrapper.length() != -1 && wrapper.length() != written) {
            throw new BinaryObjectException("Input stream length mismatch. [declaredLength=" + wrapper.length() + ", " +
                    "actualLength=" + written + "]");
        }
    }

    /**
     * Write byte array from the Blob instance.
     *
     * @param writer Writer.
     * @param blob Blob.
     */
    private static void writeByteArray(BinaryWriterEx writer, Blob blob) throws BinaryObjectException {
        try {
            int written = writer.writeByteArray(blob.getBinaryStream(), (int)blob.length());

            if ((int)blob.length() != written) {
                throw new BinaryObjectException("Blob length mismatch. [declaredLength=" + (int)blob.length() + ", " +
                        "actualLength=" + written + "]");
            }
        }
        catch (SQLException e) {
            throw new BinaryObjectException(e);
        }
    }

    /**
     * @param cls Class.
     * @return {@code true} is the type is plain (not user's custom class).
     */
    public static boolean isPlainType(Class<?> cls) {
        return cls == Boolean.class
            || cls == Byte.class
            || cls == Character.class
            || cls == Short.class
            || cls == Integer.class
            || cls == Long.class
            || cls == Float.class
            || cls == Double.class
            || cls == String.class
            || cls == BigDecimal.class
            || cls == UUID.class
            || cls == Time.class
            || cls == Timestamp.class
            || cls == java.sql.Date.class || cls == java.util.Date.class
            || cls == boolean[].class
            || cls == byte[].class
            || cls == char[].class
            || cls == short[].class
            || cls == int[].class
            || cls == long[].class
            || cls == float[].class
            || cls == double[].class
            || cls == String[].class
            || cls == BigDecimal[].class
            || cls == UUID[].class
            || cls == Time[].class
            || cls == Timestamp[].class
            || cls == java.util.Date[].class || cls == java.sql.Date[].class
            || cls == SqlInputStreamWrapper.class
            || cls == Blob.class;
    }

    /**
     * @param e Exception to convert.
     * @return IgniteQueryErrorCode.
     */
    public static int exceptionToSqlErrorCode(Throwable e) {
        if (e instanceof QueryCancelledException)
            return IgniteQueryErrorCode.QUERY_CANCELED;
        if (e instanceof IgniteSQLException)
            return ((IgniteSQLException)e).statusCode();
        else
            return IgniteQueryErrorCode.UNKNOWN;
    }

    /**
     * <p>Converts sql pattern wildcards into java regex wildcards.</p>
     * <p>Translates "_" to "." and "%" to ".*" if those are not escaped with "\" ("\_" or "\%").</p>
     * <p>All other characters are considered normal and will be escaped if necessary.</p>
     * <pre>
     * Example:
     *      som_    -->     som.
     *      so%     -->     so.*
     *      s[om]e  -->     so\[om\]e
     *      so\_me  -->     so_me
     *      some?   -->     some\?
     *      som\e   -->     som\\e
     * </pre>
     */
    public static String translateSqlWildcardsToRegex(String sqlPtrn) {
        if (F.isEmpty(sqlPtrn))
            return sqlPtrn;

        String toRegex = ' ' + sqlPtrn;

        toRegex = toRegex.replaceAll("([\\[\\]{}()*+?.\\\\\\\\^$|])", "\\\\$1");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])((?:\\\\\\\\\\\\\\\\)*)%", "$1$2.*");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])((?:\\\\\\\\\\\\\\\\)*)_", "$1$2.");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])(\\\\\\\\(?>\\\\\\\\\\\\\\\\)*\\\\\\\\)*\\\\\\\\([_|%])", "$1$2$3");

        return toRegex.substring(1);
    }
}

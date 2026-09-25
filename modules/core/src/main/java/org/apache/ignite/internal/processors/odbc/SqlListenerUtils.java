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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.binary.BinaryReaderEx;
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
        byte type = reader.readByte();

        return readObject(type, reader, binObjAllow, keepBinary);
    }

    /**
     * @param type Object type.
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(byte type, BinaryReaderEx reader, boolean binObjAllow,
                                              boolean keepBinary) throws BinaryObjectException {
        return readObject(type, reader, binObjAllow, keepBinary, true);
    }

    /**
     * @param type Object type.
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @param keepBinary Whether to deserialize objects or keep in binary format.
     * @param createByteArrayCopy Whether to return new copy or copy-on-write buffer for byte array.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(byte type, BinaryReaderEx reader, boolean binObjAllow,
        boolean keepBinary, boolean createByteArrayCopy) throws BinaryObjectException {
        if (type == GridBinaryMarshaller.BYTE_ARR && !createByteArrayCopy && reader.in().hasArray()) {
            int len = reader.in().readInt();

            int position = reader.in().position();

            reader.in().position(position + len);

            return JdbcBinaryBuffer.createReadOnly(reader.in().array(), position, len);
        }

        return reader.unmarshallJdbc(type, binObjAllow, keepBinary);
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

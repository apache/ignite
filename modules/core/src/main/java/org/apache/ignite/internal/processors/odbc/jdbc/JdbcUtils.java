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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Various JDBC utility methods.
 */
public class JdbcUtils {
    /**
     * @param writer Binary writer.
     * @param items Query results items.
     */
    public static void writeItems(BinaryWriterEx writer, List<List<Object>> items, JdbcProtocolContext protoCtx) {
        writer.writeInt(items.size());

        for (List<Object> row : items) {
            if (row != null) {
                writer.writeInt(row.size());

                for (Object obj : row)
                    writeObject(writer, obj, protoCtx);
            }
        }
    }

    /**
     * @param reader Binary reader.
     * @return Query results items.
     */
    public static List<List<Object>> readItems(BinaryReaderEx reader, JdbcProtocolContext protoCtx) {
        int rowsSize = reader.readInt();

        if (rowsSize > 0) {
            List<List<Object>> items = new ArrayList<>(rowsSize);

            for (int i = 0; i < rowsSize; ++i) {
                int colsSize = reader.readInt();

                List<Object> col = new ArrayList<>(colsSize);

                for (int colCnt = 0; colCnt < colsSize; ++colCnt)
                    col.add(readObject(reader, protoCtx, false));

                items.add(col);
            }

            return items;
        }
        else
            return Collections.emptyList();
    }

    /**
     * @param writer Binary writer.
     * @param lst List to write.
     */
    public static void writeStringCollection(BinaryWriterEx writer, Collection<String> lst) {
        if (lst == null)
            writer.writeInt(0);
        else {
            writer.writeInt(lst.size());

            for (String s : lst)
                writer.writeString(s);
        }
    }

    /**
     * @param reader Binary reader.
     * @return List of string.
     */
    public static List<String> readStringList(BinaryReaderEx reader) {
        int size = reader.readInt();

        if (size > 0) {
            List<String> lst = new ArrayList<>(size);

            for (int i = 0; i < size; ++i)
                lst.add(reader.readString());

            return lst;
        }
        else
            return Collections.emptyList();
    }

    /**
     * Read nullable Integer.
     *
     * @param reader Binary reader.
     * @return read value.
     */
    @Nullable public static Integer readNullableInteger(BinaryReaderEx reader) {
        return reader.readBoolean() ? reader.readInt() : null;
    }

    /**
     * Write nullable integer.
     *
     * @param writer Binary writer.
     * @param val Integer value..
     */
    public static void writeNullableInteger(BinaryWriterEx writer, @Nullable Integer val) {
        writer.writeBoolean(val != null);

        if (val != null)
            writer.writeInt(val);
    }

    /**
     * @param reader Reader.
     * @param protoCtx Protocol context.
     * @param createByteArrayCopy Whether to create new copy or copy-on-write buffer for byte array.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(
            BinaryReaderEx reader,
            JdbcProtocolContext protoCtx,
            boolean createByteArrayCopy
    ) throws BinaryObjectException {
        return SqlListenerUtils.readObject(reader,
                protoCtx.isFeatureSupported(JdbcThinFeature.CUSTOM_OBJECT), protoCtx.keepBinary(), createByteArrayCopy);
    }

    /**
     * @param reader Reader.
     * @param protoCtx Protocol context.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(
        BinaryReaderEx reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        return readObject(reader, protoCtx, true);
    }

    /**
     * @param writer Writer.
     * @param obj Object to write.
     * @param protoCtx Protocol context.
     * @throws BinaryObjectException On error.
     */
    public static void writeObject(
        BinaryWriterEx writer,
        @Nullable Object obj,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        if (obj == null) {
            writer.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        SqlListenerUtils.writeObject(writer, obj, protoCtx.isFeatureSupported(JdbcThinFeature.CUSTOM_OBJECT));
    }
}

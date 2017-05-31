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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;

/**
 * Various JDBC utility methods.
 */
public class JdbcUtils {
    /** */
    private static final ThreadLocal<Boolean> isReadSqlObject = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * @param writer Binari writer.
     * @param items Query results items.
     */
    public static void writeItems(BinaryWriterExImpl writer, List<List<Object>> items) {
        writer.writeInt(items.size());

        for (List<Object> row : items) {
            if (row != null) {
                writer.writeInt(row.size());

                for (Object obj : row)
                    SqlListenerUtils.writeObject(writer, obj, false);
            }
        }
    }

    /**
     * @param reader Binary reader.
     * @return Query results items.
     */
    public static List<List<Object>> readItems(BinaryReaderExImpl reader) {
        int rowsSize = reader.readInt();

        if (rowsSize > 0) {
            List<List<Object>> items = new ArrayList<>(rowsSize);

            for (int i = 0; i < rowsSize; ++i) {
                int colsSize = reader.readInt();

                List<Object> col = new ArrayList<>(colsSize);

                for (int colCnt = 0; colCnt < colsSize; ++colCnt) {
                    if (isReadSqlObject.get())
                        col.add(readSqlObject(reader));
                    else
                        col.add(SqlListenerUtils.readObject(reader, false));
                }

                items.add(col);
            }

            return items;
        } else
            return Collections.emptyList();
    }

    /**
     * @param reader Binary reader.
     * @return Query results items.
     */
    public static Object readSqlObject(BinaryReaderExImpl reader) {
        Object obj = SqlListenerUtils.readObject(reader, false);

        if (obj == null)
            return null;

        if (obj.getClass() == java.util.Date.class)
            return new java.sql.Date(((java.util.Date)obj).getTime());

        if (obj.getClass() == java.util.Date[].class) {
            java.util.Date[] arr = (java.util.Date[])obj;

            java.sql.Date[] sqlArr = new java.sql.Date[arr.length];

            for (int i = 0; i < arr.length; ++i)
                sqlArr[i] = new java.sql.Date(arr[i].getTime());

            return sqlArr;
        }

        return obj;
    }

    /**
     * @param readSqlObj when {@code true} {@code readItems} method returns {@code java.sql.Date} instead of
     *      {@code java.util.Date}
     */
    public static void setReadSqlObject(boolean readSqlObj) {
        isReadSqlObject.set(readSqlObj);
    }

}

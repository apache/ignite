/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;

/**
 * Various JDBC utility methods.
 */
public class JdbcUtils {
    /**
     * @param writer Binary writer.
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

                for (int colCnt = 0; colCnt < colsSize; ++colCnt)
                    col.add(SqlListenerUtils.readObject(reader, false));

                items.add(col);
            }

            return items;
        } else
            return Collections.emptyList();
    }

    /**
     * @param writer Binary writer.
     * @param lst List to write.
     */
    public static void writeStringCollection(BinaryWriterExImpl writer, Collection<String> lst) {
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
    public static List<String> readStringList(BinaryReaderExImpl reader) {
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
}

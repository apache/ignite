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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC SQL query with parameters.
 */
public class OdbcQuery {
    /** Query SQL. */
    private String sql;

    /** Arguments. */
    private Object[] args;

    /**
     * Default constructor is used for serialization.
     */
    public OdbcQuery() {
        // No-op.
    }

    /**
     * @return Query SQL string.
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Query arguments.
     */
    public Object[] args() {
        return args;
    }

    /**
     * Writes fields to provided writer.
     *
     * @param writer Binary object writer.
     */
    public void writeBinary(BinaryWriterExImpl writer) {
        writer.writeString(sql);

        if (args == null || args.length == 0)
            writer.writeInt(0);
        else {
            writer.writeInt(args.length);

            for (Object arg : args)
                SqlListenerUtils.writeObject(writer, arg, false);
        }
    }

    /**
     * Reads fields from provided reader.
     *
     * @param reader Binary object reader.
     */
    public void readBinary(BinaryReaderExImpl reader) {
        sql = reader.readString();

        int argsNum = reader.readInt();

        args = new Object[argsNum];

        for (int i = 0; i < argsNum; ++i)
            args[i] = SqlListenerUtils.readObject(reader, false);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQuery.class, this);
    }
}

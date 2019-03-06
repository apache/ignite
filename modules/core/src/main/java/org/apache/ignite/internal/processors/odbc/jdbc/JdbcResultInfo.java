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

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC statement result information. Keeps statement type (SELECT or UPDATE) and
 * queryId or update count (depends on statement type).
 */
public class JdbcResultInfo implements JdbcRawBinarylizable {
    /** Query flag. */
    private boolean isQuery;

    /** Update count. */
    private long updCnt;

    /** Cursor ID. */
    private long cursorId;

    /**
     * Default constructor is used for serialization.
     */
    JdbcResultInfo() {
        // No-op.
    }

    /**
     * @param isQuery Query flag.
     * @param updCnt Update count.
     * @param cursorId  Cursor ID.
     */
    public JdbcResultInfo(boolean isQuery, long updCnt, long cursorId) {
        this.isQuery = isQuery;
        this.updCnt = updCnt;
        this.cursorId = cursorId;
    }

    /**
     * @return Query flag.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * @return Update count.
     */
    public long updateCount() {
        return updCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) {
        writer.writeBoolean(isQuery);
        writer.writeLong(updCnt);
        writer.writeLong(cursorId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) {
        isQuery = reader.readBoolean();
        updCnt = reader.readLong();
        cursorId = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcResultInfo.class, this);
    }
}

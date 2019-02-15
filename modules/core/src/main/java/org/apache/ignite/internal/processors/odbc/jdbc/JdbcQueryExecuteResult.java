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

import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query execute result.
 */
public class JdbcQueryExecuteResult extends JdbcResult {
    /** Cursor ID. */
    private long cursorId;

    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;

    /**
     * Constructor.
     */
    JdbcQueryExecuteResult() {
        super(QRY_EXEC);
    }

    /**
     * @param cursorId Cursor ID.
     * @param items Query result rows.
     * @param last Flag indicates the query has no unfetched results.
     */
    JdbcQueryExecuteResult(long cursorId, List<List<Object>> items, boolean last) {
        super(QRY_EXEC);

        this.cursorId = cursorId;
        this.items = items;
        this.last = last;
        this.isQuery = true;
    }

    /**
     * @param cursorId Cursor ID.
     * @param updateCnt Update count for DML queries.
     */
    public JdbcQueryExecuteResult(long cursorId, long updateCnt) {
        super(QRY_EXEC);

        this.cursorId = cursorId;
        this.last = true;
        this.isQuery = false;
        this.updateCnt = updateCnt;
    }

    /**
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * @return Query result rows.
     */
    public List<List<Object>> items() {
        return items;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Flag indicating the query is SELECT query. {@code false} for DML/DDL queries.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * @return Update count for DML queries.
     */
    public long updateCount() {
        return updateCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeLong(cursorId);
        writer.writeBoolean(isQuery);

        if (isQuery) {
            assert items != null;

            writer.writeBoolean(last);

            JdbcUtils.writeItems(writer, items);
        }
        else
            writer.writeLong(updateCnt);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        cursorId = reader.readLong();
        isQuery = reader.readBoolean();

        if (isQuery) {
            last = reader.readBoolean();

            items = JdbcUtils.readItems(reader);
        }
        else {
            last = true;

            updateCnt = reader.readLong();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteResult.class, this);
    }
}

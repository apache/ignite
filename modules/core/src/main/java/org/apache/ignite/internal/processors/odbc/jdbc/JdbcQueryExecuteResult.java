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
    /** Query ID. */
    private long queryId;

    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;

    /**
     * Condtructor.
     */
    JdbcQueryExecuteResult() {
        super(QRY_EXEC);
    }

    /**
     * @param queryId Query ID.
     * @param items Query result rows.
     * @param last Flag indicates the query has no unfetched results.
     */
    JdbcQueryExecuteResult(long queryId, List<List<Object>> items, boolean last) {
        super(QRY_EXEC);

        this.queryId = queryId;
        this.items = items;
        this.last = last;
        this.isQuery = true;
    }

    /**
     * @param queryId Query ID.
     * @param updateCnt Update count for DML queries.
     */
    public JdbcQueryExecuteResult(long queryId, long updateCnt) {
        super(QRY_EXEC);

        this.queryId = queryId;
        this.last = true;
        this.isQuery = false;
        this.updateCnt = updateCnt;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
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

        writer.writeLong(queryId);
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

        queryId = reader.readLong();
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

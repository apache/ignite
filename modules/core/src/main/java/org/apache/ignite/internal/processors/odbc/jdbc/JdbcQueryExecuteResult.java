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

/**
 * SQL listener query execute result.
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

    private int updateCount;

    /**
     *
     */
    public JdbcQueryExecuteResult() {
        super(QRY_EXEC);
    }

    /**
     * @param queryId Query ID.
     * @param items Query result rows.
     * @param last Flag indicates the query has no unfetched results.
     * @param isQuery Flag indicates the query is SELECT query. {@code false} for DML/DDL queries
     */
    public JdbcQueryExecuteResult(long queryId, List<List<Object>> items, boolean last, boolean isQuery) {
        super(QRY_EXEC);

        this.queryId = queryId;
        this.items = items;
        this.last = last;
        this.isQuery = isQuery;
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

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeLong(queryId);
        writer.writeBoolean(last);
        writer.writeBoolean(isQuery);

        assert items != null;

        JdbcUtils.writeItems(writer, items);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        queryId = reader.readLong();
        last = reader.readBoolean();
        isQuery = reader.readBoolean();

        items = JdbcUtils.readItems(reader);
    }
}

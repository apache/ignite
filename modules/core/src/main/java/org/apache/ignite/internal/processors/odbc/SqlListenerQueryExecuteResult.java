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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * SQL listener query execute result.
 */
public class SqlListenerQueryExecuteResult implements RawBinarylizable {
    /** Query ID. */
    private long queryId;

    /** Fields metadata. */
    private List<SqlListenerColumnMeta> meta;

    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /**
     *
     */
    public SqlListenerQueryExecuteResult() {
    }

    /**
     * @param queryId Query ID.
     * @param meta Fields metadata.
     * @param items Query result rows.
     * @param last Flag indicates the query has no unfetched results.
     * @param isQuery Flag indicates the query is SELECT query. {@code false} for DML/DDL queries
     */
    public SqlListenerQueryExecuteResult(long queryId, List<SqlListenerColumnMeta> meta, List<List<Object>> items, boolean last, boolean isQuery) {
        this.queryId = queryId;
        this.meta = meta;
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
     * @return Query results metadata.
     */
    public List<SqlListenerColumnMeta> meta() {
        return meta;
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
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        writer.writeLong(queryId);
        writer.writeBoolean(last);
        writer.writeBoolean(isQuery);

        SqlListenerAbstractMessageParser.writeColumnsMeta(writer, objWriter, meta);

        assert items != null;

        SqlListenerAbstractMessageParser.writeItems(writer, objWriter, items);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        queryId = reader.readLong();
        last = reader.readBoolean();
        isQuery = reader.readBoolean();

        meta = SqlListenerAbstractMessageParser.readColumnsMeta(reader, objReader);

        items = SqlListenerAbstractMessageParser.readItems(reader, objReader);
    }
}

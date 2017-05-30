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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * SQL listener query fetch result.
 */
class JdbcQueryCursor {
    /** Query ID. */
    private final long queryId;

    /** Fetch size. */
    private int pageSize;

    /** Max rows. */
    private final long maxRows;

    /** Number of fetched rows. */
    private long fetched;

    /** Query result rows. */
    private final QueryCursorImpl<List<Object>> cur;

    /** Query results iterator. */
    private final Iterator<List<Object>> iter;

    /**
     * @param queryId Query ID.
     * @param pageSize Fetch size.
     * @param maxRows Max rows.
     * @param cur Query cursor.
     */
    JdbcQueryCursor(long queryId, int pageSize, int maxRows, QueryCursorImpl<List<Object>> cur) {
        this.queryId = queryId;
        this.pageSize = pageSize;
        this.maxRows = maxRows;
        this.cur = cur;

        iter = cur.iterator();
    }

    /**
     * @return List of the rows.
     */
    List<List<Object>> fetchRows() {
        List<List<Object>> items = new ArrayList<>();

        int fetchSize0 = (maxRows > 0) ? (int)Math.min(pageSize, maxRows - fetched) : pageSize;

        for (; fetched < fetchSize0 && iter.hasNext(); ++fetched)
            items.add(iter.next());

        return items;
    }

    /**
     * @return Query metadata.
     */
    List<JdbcColumnMeta> meta() {
        List<?> meta = cur.fieldsMeta();

        List<JdbcColumnMeta> res = new ArrayList<>();

        if (meta != null) {
            for (Object info : meta) {
                assert info instanceof GridQueryFieldMetadata;

                res.add(new JdbcColumnMeta((GridQueryFieldMetadata)info));
            }
        }

        return res;
    }

    /**
     * @return {@code true} if the cursor has more rows
     */
    boolean hasNext() {
        return iter.hasNext() && !(maxRows > 0 && fetched >= maxRows);
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * Close the cursor.
     */
    public void close() {
        cur.close();
    }

    /**
     * @param pageSize New fetch size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return {@code true} if this cursor corresponds to a {@link ResultSet} as a result of query,
     * {@code false} if query was modifying operation like INSERT, UPDATE, or DELETE.
     */
    public boolean isQuery() {
        return cur.isQuery();
    }
}

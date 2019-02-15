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
        int fetchSize = (maxRows > 0) ? (int)Math.min(pageSize, maxRows - fetched) : pageSize;

        List<List<Object>> items = new ArrayList<>(fetchSize);

        for (int i = 0; i < fetchSize && iter.hasNext(); i++) {
            items.add(iter.next());

            fetched++;
        }

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

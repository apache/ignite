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

package org.apache.ignite.internal.processors.odbc.odbc;

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

/**
 * ODBC result set
 */
public class OdbcQueryResults {
    /** Current cursor. */
    private final List<FieldsQueryCursor<List<?>>> cursors;

    /** Rows affected. */
    private final long[] rowsAffected;

    /** Current result set. */
    private OdbcResultSet currentResultSet;

    /** Current result set index. */
    private int nextResultSetIdx;

    /** Client version. */
    private ClientListenerProtocolVersion ver;

    /**
     * @param cursors Result set cursors.
     * @param ver Client version.
     */
    OdbcQueryResults(List<FieldsQueryCursor<List<?>>> cursors, ClientListenerProtocolVersion ver) {
        this.cursors = cursors;
        this.nextResultSetIdx = 0;
        this.ver = ver;

        rowsAffected = new long[cursors.size()];

        for (int i = 0; i < cursors.size(); ++i)
            rowsAffected[i] = OdbcUtils.rowsAffected(cursors.get(i));

        nextResultSet();
    }

    /**
     * Get affected rows for all result sets.
     * @return List of numbers of table rows affected by every statement.
     */
    public long[] rowsAffected() {
        return rowsAffected;
    }

    /**
     * @return {@code true} if any of the result sets still has non-fetched rows.
     */
    public boolean hasUnfetchedRows() {
        if (currentResultSet != null && currentResultSet.hasUnfetchedRows())
            return true;

        for (FieldsQueryCursor<List<?>> cursor0 : cursors) {
            QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>)cursor0;

            if (cursor.isQuery())
                return true;
        }
        return false;
    }

    /**
     * Close all cursors.
     */
    public void closeAll() {
        for (FieldsQueryCursor<List<?>> cursor : cursors)
            cursor.close();
    }

    /**
     * @return Current result set.
     */
    public OdbcResultSet currentResultSet() {
        return currentResultSet;
    }

    /**
     * Move to next result set.
     */
    public void nextResultSet() {
        currentResultSet = null;

        if (nextResultSetIdx != cursors.size()) {
            currentResultSet = new OdbcResultSet(cursors.get(nextResultSetIdx), ver);
            ++nextResultSetIdx;
        }
    }
}

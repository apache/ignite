/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.action.query;

import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for query result.
 */
public class QueryResult {
    /** Node where query executed. */
    private String resNodeId;

    /** Query columns descriptors. */
    private List<QueryField> cols;

    /** Rows fetched from query. */
    private List<Object[]> rows;

    /** Whether query has more rows to fetch. */
    private boolean hasMore;

    /** Query duration */
    private long duration;

    /** Cursor ID. */
    private String cursorId;

    /**
     * @return Result node ID.
     */
    public String getResultNodeId() {
        return resNodeId;
    }

    /**
     * @param resNodeId Response node id.
     * @return {@code This} for chaining method calls.
     */
    public QueryResult setResultNodeId(String resNodeId) {
        this.resNodeId = resNodeId;

        return this;
    }

    /**
     * @return List of columns.
     */
    public List<QueryField> getColumns() {
        return cols;
    }

    /**
     * @param cols Columns.
     * @return {@code This} for chaining method calls.
     */
    public QueryResult setColumns(List<QueryField> cols) {
        this.cols = cols;

        return this;
    }

    /**
     * @return List of rows.
     */
    public List<Object[]> getRows() {
        return rows;
    }

    /**
     * @param rows Rows.
     * @return {@code This} for chaining method calls.
     */
    public QueryResult setRows(List<Object[]> rows) {
        this.rows = rows;

        return this;
    }

    /**
     * @return @{code true} if query has more results.
     */
    public boolean isHasMore() {
        return hasMore;
    }

    /**
     * @param hasMore Has more.
     * @return {@code This} for chaining method calls.
     */
    public QueryResult setHasMore(boolean hasMore) {
        this.hasMore = hasMore;

        return this;
    }

    /**
     * @return Query duration.
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @param duration Duration.
     * @return {@code This} for chaining method calls.
     */
    public QueryResult setDuration(long duration) {
        this.duration = duration;

        return this;
    }

    /**
     * @return Cursor ID.
     */
    public String getCursorId() {
        return cursorId;
    }

    /**
     * @param cursorId Cursor ID.
     * @return {@code This} for chaining method calls.
     */
    public QueryResult setCursorId(String cursorId) {
        this.cursorId = cursorId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryResult.class, this);
    }
}

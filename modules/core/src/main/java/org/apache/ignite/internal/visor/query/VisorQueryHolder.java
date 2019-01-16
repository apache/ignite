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

package org.apache.ignite.internal.visor.query;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.GridQueryCancel;

/**
 * Holds identify information of executing query and its result.
 */
public class VisorQueryHolder {
    /** Prefix for node local key for SQL queries. */
    private static final String SQL_QRY_PREFIX = "VISOR_SQL_QUERY";

    /** Prefix for node local key for SCAN queries. */
    private static final String SCAN_QRY_PREFIX = "VISOR_SCAN_QUERY";

    /** Query ID for extraction query data result. */
    private final String qryId;

    /** Cancel query object. */
    private final GridQueryCancel cancel;

    /** Size of result to extract by query. */
    private final int pageSize;

    /** Query start time in ms. */
    private final long start;

    /** Wrapper for query cursor. */
    private VisorQueryCursor<?> cur;

    /** Query column descriptors. */
    private List<VisorQueryField> cols;

    /** Rows fetched from query. */
    private List<Object[]> rows;

    /** Error in process of query result receiving. */
    private Throwable err;

    /** Query duration in ms. */
    private long duration = -1;

    /** Flag indicating that this cursor was read from last check. */
    private volatile boolean accessed;

    /**
     * @param qryId Query ID.
     * @return {@code true} if holder contains SQL query.
     */
    public static boolean isSqlQuery(String qryId) {
        return qryId.startsWith(SQL_QRY_PREFIX);
    }

    /**
     * Constructor.
     *
     * @param sqlQry Flag indicating that holder contains SQL or SCAN query.
     * @param cur Wrapper for query cursor.
     * @param pageSize Page size to fetch.
     * @param cancel Cancel object.
     */
    VisorQueryHolder(boolean sqlQry, VisorQueryCursor<?> cur, int pageSize, GridQueryCancel cancel) {
        this.cur = cur;
        this.pageSize = pageSize;
        this.cancel = cancel;

        // Generate query ID to store query cursor in node local storage.
        qryId = (sqlQry ? SQL_QRY_PREFIX : SCAN_QRY_PREFIX) + "-" + UUID.randomUUID();

        start = System.currentTimeMillis();
    }

    /**
     * @return Query ID for extraction query data result.
     */
    public String getQueryID() {
        return qryId;
    }

    /**
     * @return Wrapper for query cursor.
     */
    public VisorQueryCursor<?> getCursor() {
        return cur;
    }

    /**
     * Set wrapper for query cursor.
     *
     * @param cur Wrapper for query cursor.
     */
    public void setCursor(VisorQueryCursor<?> cur) {
        this.cur = cur;
    }

    /**
     * @return Query column descriptors.
     */
    public List<VisorQueryField> getColumns() {
        return cols;
    }

    /**
     * Get query column descriptors.
     *
     * @param cols Query column descriptors.
     */
    public void setColumns(List<VisorQueryField> cols) {
        this.cols = cols;
    }

    /**
     * @return Size of result to extract by query.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Cancel query.
     */
    public void cancelQuery() {
        if (cur != null)
            cur.close();

        if (cancel != null)
            cancel.cancel();
    }

    /**
     * @return Rows fetched from query or `null` if result is not fetched yet.
     */
    public List<Object[]> getRows() {
        List<Object[]> res = rows;
        rows = null;

        return res;
    }

    /**
     * Set fetched from query rows.
     *
     * @param rows Rows fetched from query.
     */
    public void setRows(List<Object[]> rows) {
        duration = System.currentTimeMillis() - start;

        this.rows = rows;
    }

    /**
     * @return Error in process of query result receiving.
     */
    public Throwable getErr() {
        return err;
    }

    /**
     * Set error caught during query execution.
     *
     * @param err Error caught during query execution.
     */
    public void setError(Throwable err) {
        this.err = err;

        if (cur != null)
            cur.close();
    }

    /**
     * @return Flag indicating that this future was read from last check..
     */
    public boolean accessed() {
        return accessed;
    }

    /**
     * @param accessed New accessed.
     */
    public void accessed(boolean accessed) {
        this.accessed = accessed;
    }

    /**
     * @return Duration of query execution.
     */
    public long duration() {
        if (duration > 0)
            return duration;

        return System.currentTimeMillis() - start;
    }
}

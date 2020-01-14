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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.GridQueryCancel;

/**
 * Holds identify information of executing query and its result.
 */
public class VisorQueryHolder implements AutoCloseable {
    /** Prefix for node local key for SQL queries. */
    private static final String SQL_QRY_PREFIX = "VISOR_SQL_QUERY";

    /** Prefix for node local key for SCAN queries. */
    private static final String SCAN_QRY_PREFIX = "VISOR_SCAN_QUERY";

    /** Query ID for extraction query data result. */
    private final String qryId;

    /** Cancel query object. */
    private final GridQueryCancel cancel;

    /** Query column descriptors. */
    private volatile List<VisorQueryField> cols;

    /** Error in process of query result receiving. */
    private volatile Throwable err;

    /** Query duration in ms. */
    private volatile long duration;

    /** Flag indicating that this cursor was read from last check. */
    private volatile boolean accessed;

    /** Query cursor. */
    private volatile QueryCursor cur;

    /** Result set iterator. */
    private volatile Iterator itr;

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
     * @param cur Query cursor.
     * @param cancel Cancel object.
     */
    VisorQueryHolder(boolean sqlQry, QueryCursor cur, GridQueryCancel cancel) {
        this.cur = cur;
        this.cancel = cancel;

        // Generate query ID to store query cursor in node local storage.
        qryId = (sqlQry ? SQL_QRY_PREFIX : SCAN_QRY_PREFIX) + "-" + UUID.randomUUID();
    }

    /**
     * @return Query ID for extraction query data result.
     */
    public String getQueryID() {
        return qryId;
    }

    /**
     * @return Result set iterator.
     */
    public synchronized Iterator getIterator() {
        assert cur != null;

        if (itr == null)
            itr = cur.iterator();

        return itr;
    }

    /**
     * @return Query column descriptors.
     */
    public List<VisorQueryField> getColumns() {
        return cols;
    }

    /**
     * Complete query execution.
     *
     * @param cur Query cursor.
     * @param duration Duration of query execution.
     * @param cols Query column descriptors.
     */
    public void complete(QueryCursor cur, long duration, List<VisorQueryField> cols) {
        this.cur = cur;
        this.duration = duration;
        this.cols = cols;
        this.accessed = false;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (cur != null)
            cur.close();

        if (cancel != null)
            cancel.cancel();
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
    public boolean isAccessed() {
        return accessed;
    }

    /**
     * @param accessed New accessed.
     */
    public void setAccessed(boolean accessed) {
        this.accessed = accessed;
    }

    /**
     * @return Duration of query execution.
     */
    public long duration() {
        return duration;
    }
}

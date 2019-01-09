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
import org.apache.ignite.internal.processors.query.GridQueryCancel;

/**
 * Holds identify information of executing query and its result.
 */
public class VisorQueryHolder {
    /** Query ID for extraction query data result. */
    private String qryId;

    /** Wrapper for query cursor. */
    private VisorQueryCursor<?> cur;

    /** Cancel query object. */
    private GridQueryCancel cancel;

    /** Rows fetched from query. */
    private List<Object[]> rows;

    /** Size of result to extract by query. */
    private int pageSize;

    /** Error in process of query result receiving. */
    private Throwable err;

    /** Query start time in ms. */
    private long start;

    /**
     * Constructor.
     *
     * @param qryId Query ID for extraction query data result.
     * @param cur Wrapper for query cursor.
     */
    VisorQueryHolder(String qryId, VisorQueryCursor<?> cur, int pageSize, GridQueryCancel cancel) {
        this.qryId = qryId;
        this.cur = cur;
        this.pageSize = pageSize;
        this.cancel = cancel;
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
        cur.accessed(true);

        return cur;
    }

    /**
     * @return Size of result to extract by query.
     */
    public int getPageSize() {
        return pageSize;
    }

    public void cancelQuery() {
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
     * @return Error in process of query result receiving.
     */
    public Throwable getErr() {
        return err;
    }

    /**
     * Set error in process of query result receiving.
     * @param err Error in process of query result receiving.
     */
    public void setErr(Throwable err) {
        this.err = err;
        cur.close();
    }

    /**
     * Set fetched from query rows.
     *
     * @param rows Rows fetched from query.
     */
    public void setRows(List<Object[]> rows) {
        this.rows = rows;
    }

    /**
     * @return Duration of query execution.
     */
    public long duration() {
        return System.currentTimeMillis() - start;
    }
}

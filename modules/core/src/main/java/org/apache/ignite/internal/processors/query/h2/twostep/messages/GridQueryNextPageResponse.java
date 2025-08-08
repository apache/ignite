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

package org.apache.ignite.internal.processors.query.h2.twostep.messages;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Next page response.
 */
@IgniteCodeGeneratingFail
public class GridQueryNextPageResponse implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 0, method = "queryRequestId")
    private long qryReqId;

    /** */
    @Order(1)
    private int segmentId;

    /** */
    @Order(value = 2, method = "query")
    private int qry;

    /** */
    @Order(3)
    private int page;

    /** */
    @Order(4)
    private int allRows;

    /** */
    @Order(value = 5, method = "columns")
    private int cols;

    /** */
    @Order(value = 6, method = "values")
    private Collection<Message> vals;

    /**
     * Note, columns count in plain row can differ from {@link #cols}.
     * See {@code org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory#toMessages}.
     * See javadoc for {@code org.h2.result.ResultInterface#getVisibleColumnCount()} and {@code org.h2.result.ResultInterface#currentRow()}.
     */
    private transient Collection<?> plainRows;

    /** */
    @Order(7)
    private AffinityTopologyVersion retry;

    /** Retry cause description. */
    @Order(8)
    private String retryCause;

    /** Last page flag. */
    @Order(9)
    private boolean last;

    /**
     * Empty constructor.
     */
    public GridQueryNextPageResponse() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param segmentId Index segment ID.
     * @param qry Query.
     * @param page Page.
     * @param allRows All rows count.
     * @param cols Number of columns in row.
     * @param vals Values for rows in this page added sequentially.
     * @param plainRows Not marshalled rows for local node.
     * @param last Last page flag.
     */
    public GridQueryNextPageResponse(long qryReqId, int segmentId, int qry, int page, int allRows, int cols,
        Collection<Message> vals, Collection<?> plainRows, boolean last) {
        assert vals != null ^ plainRows != null;
        assert cols > 0 : cols;

        this.qryReqId = qryReqId;
        this.segmentId = segmentId;
        this.qry = qry;
        this.page = page;
        this.allRows = allRows;
        this.cols = cols;
        this.vals = vals;
        this.plainRows = plainRows;
        this.last = last;
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @param qryReqId New query request ID.
     */
    public void queryRequestId(long qryReqId) {
        this.qryReqId = qryReqId;
    }

    /**
     * @return Index segment ID.
     */
    public int segmentId() {
        return segmentId;
    }

    /**
     * @param segmentId New index segment ID.
     */
    public void segmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    /**
     * @return Query.
     */
    public int query() {
        return qry;
    }

    /**
     * @param qry New query.
     */
    public void query(int qry) {
        this.qry = qry;
    }

    /**
     * @return Page.
     */
    public int page() {
        return page;
    }

    /**
     * @param page New page.
     */
    public void page(int page) {
        this.page = page;
    }

    /**
     * @return All rows.
     */
    public int allRows() {
        return allRows;
    }

    /**
     * @param allRows New all rows.
     */
    public void allRows(int allRows) {
        this.allRows = allRows;
    }

    /**
     * @return Columns in row.
     */
    public int columns() {
        return cols;
    }

    /**
     * @param cols New columns in row.
     */
    public void columns(int cols) {
        this.cols = cols;
    }

    /**
     * @return Values.
     */
    public Collection<Message> values() {
        return vals;
    }

    /**
     * @param vals New values.
     */
    public void values(Collection<Message> vals) {
        this.vals = vals;
    }

    /**
     * @return Plain rows.
     */
    public Collection<?> plainRows() {
        return plainRows;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 109;
    }

    /**
     * @return Retry topology version.
     */
    public AffinityTopologyVersion retry() {
        return retry;
    }

    /**
     * @param retry Retry topology version.
     */
    public void retry(AffinityTopologyVersion retry) {
        this.retry = retry;
    }

    /**
     * @return Retry Ccause message.
     */
    public String retryCause() {
        return retryCause;
    }

    /**
     * @param retryCause Retry Ccause message.
     */
    public void retryCause(String retryCause) {
        this.retryCause = retryCause;
    }

    /**
     * @return Last page flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @param last Last page flag.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryNextPageResponse.class, this,
            "valsSize", vals != null ? vals.size() : 0,
            "rowsSize", plainRows != null ? plainRows.size() : 0);
    }
}

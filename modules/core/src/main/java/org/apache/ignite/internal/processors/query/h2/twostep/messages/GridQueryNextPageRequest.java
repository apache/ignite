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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Request to fetch next page.
 */
public class GridQueryNextPageRequest implements Message {
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
    private int pageSize;

    /** */
    @Order(value = 4, method = "getFlags")
    private byte flags;

    /**
     * Default constructor.
     */
    public GridQueryNextPageRequest() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param qry Query.
     * @param segmentId Index segment ID.
     * @param pageSize Page size.
     * @param flags Flags.
     */
    public GridQueryNextPageRequest(long qryReqId, int qry, int segmentId, int pageSize, byte flags) {
        this.qryReqId = qryReqId;
        this.qry = qry;
        this.segmentId = segmentId;
        this.pageSize = pageSize;
        this.flags = flags;
    }

    /**
     * @return Flags.
     */
    public byte getFlags() {
        return flags;
    }

    /**
     * @param flags New flags.
     */
    public void getFlags(byte flags) {
        this.flags = flags;
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

    /** @return Index segment ID */
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
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param pageSize New page size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryNextPageRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 108;
    }
}

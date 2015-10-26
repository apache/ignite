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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Range response message.
 */
public class GridH2IndexRangeResponse implements Message {
    /** */
    public static final byte STATUS_OK = 0;

    /** */
    public static final byte STATUS_ERROR = 1;

    /** */
    public static final byte STATUS_NOT_FOUND = 2;

    /** */
    private UUID originNodeId;

    /** */
    private long qryId;

    /** */
    /** */
    @GridDirectCollection(Message.class)
    private List<GridH2RowRange> ranges;

    /** */
    private byte status;

    /** */
    private String err;

    /**
     * @param ranges Ranges.
     */
    public void ranges(List<GridH2RowRange> ranges) {
        this.ranges = ranges;
    }

    /**
     * @return Ranges.
     */
    public List<GridH2RowRange> ranges() {
        return ranges;
    }

    /**
     * @return Origin node ID.
     */
    public UUID originNodeId() {
        return originNodeId;
    }

    /**
     * @param originNodeId Origin node ID.
     */
    public void originNodeId(UUID originNodeId) {
        this.originNodeId = originNodeId;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     */
    public void queryId(long qryId) {
        this.qryId = qryId;
    }

    /**
     * @param err Error message.
     */
    public void error(String err) {
        this.err = err;
    }

    /**
     * @return Error message or {@code null} if everything is ok.
     */
    public String error() {
        return err;
    }

    /**
     * @param status Status.
     */
    public void status(byte status) {
        this.status = status;
    }

    /**
     * @return Status.
     */
    public byte status() {
        return status;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -24;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }
}

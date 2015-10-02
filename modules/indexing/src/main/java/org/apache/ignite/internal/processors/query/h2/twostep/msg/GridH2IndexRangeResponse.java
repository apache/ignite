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
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 */
public class GridH2IndexRangeResponse implements Message {
    /** */
    public static final byte STATUS_OK = 0;

    /** */
    public static final byte STATUS_ERROR = 1;

    /** */
    public static final byte STATUS_RETRY = 1;

    /** */
    private long id;

    /** Sizes for returned ranges. */
    private int[] sizes;

    /** Row for one or multiple ranges. */
    @GridDirectCollection(Message.class)
    private List<GridH2RowMessage> rows;

    /** */
    private byte status;

    /** */
    private String err;

    /** */
    private long sourceId;

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
     * @param id ID.
     */
    public void id(long id) {
        this.id = id;
    }

    /**
     * @return ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Sizes.
     */
    public int[] sizes() {
        return sizes;
    }

    /**
     * @param sizes Sizes.
     */
    public void sizes(int[] sizes) {
        this.sizes = sizes;
    }

    /**
     * @return Rows.
     */
    public List<GridH2RowMessage> rows() {
        return rows;
    }

    /**
     * @param rows Rows.
     */
    public void rows(List<GridH2RowMessage> rows) {
        this.rows = rows;
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

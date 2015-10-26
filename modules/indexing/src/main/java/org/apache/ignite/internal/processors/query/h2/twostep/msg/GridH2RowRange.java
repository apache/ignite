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
 * Range of rows.
 */
public class GridH2RowRange implements Message {
    /** */
    private static int FLAG_PARTIAL = 1;

    /** */
    private int rangeId;

    /** */
    @GridDirectCollection(Message.class)
    private List<GridH2RowMessage> rows;

    /** */
    private byte flags;

    /**
     * @param rangeId Range ID.
     */
    public void rangeId(int rangeId) {
        this.rangeId = rangeId;
    }

    /**
     * @return Range ID.
     */
    public int rangeId() {
        return rangeId;
    }

    /**
     * @param rows Rows.
     */
    public void rows(List<GridH2RowMessage> rows) {
        this.rows = rows;
    }

    /**
     * @return Rows.
     */
    public List<GridH2RowMessage> rows() {
        return rows;
    }

    /**
     * Sets that this is a partial range.
     */
    public void setPartial() {
        flags |= FLAG_PARTIAL;
    }

    /**
     * @return {@code true} If this is a partial range.
     */
    public boolean isPartial() {
        return (flags & FLAG_PARTIAL) == FLAG_PARTIAL;
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
        return -27;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }
}

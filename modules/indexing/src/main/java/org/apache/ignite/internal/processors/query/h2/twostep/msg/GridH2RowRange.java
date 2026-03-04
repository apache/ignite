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

import java.util.List;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Range of rows.
 */
public class GridH2RowRange implements Message {
    /** */
    private static final int FLAG_PARTIAL = 1;

    /** */
    @Order(0)
    int rangeId;

    /** */
    @GridToStringInclude
    @Order(1)
    List<GridH2RowMessage> rows;

    /** */
    @Order(2)
    byte flags;

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
    @Override public short directType() {
        return -34;
    }

    /**
     * @return Number of rows.
     */
    public int rowsSize() {
        return rows == null ? 0 : rows.size();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2RowRange.class, this, "rowsSize", rowsSize());
    }
}

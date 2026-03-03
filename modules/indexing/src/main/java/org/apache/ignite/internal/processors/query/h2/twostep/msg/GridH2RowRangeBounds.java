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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Bounds of row range.
 */
public class GridH2RowRangeBounds implements Message {
    /** */
    @Order(0)
    int rangeId;

    /** */
    @Order(1)
    GridH2RowMessage first;

    /** */
    @Order(2)
    GridH2RowMessage last;

    /**
     * @param rangeId Range ID.
     * @param first First.
     * @param last Last.
     * @return Range bounds.
     */
    public static GridH2RowRangeBounds rangeBounds(int rangeId, GridH2RowMessage first, GridH2RowMessage last) {
        GridH2RowRangeBounds res = new GridH2RowRangeBounds();

        res.rangeId(rangeId);
        res.first(first);
        res.last(last);

        return res;
    }

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
     * @param first First.
     */
    public void first(GridH2RowMessage first) {
        this.first = first;
    }

    /**
     * @return First.
     */
    public GridH2RowMessage first() {
        return first;
    }

    /**
     * @param last Last.
     */
    public void last(GridH2RowMessage last) {
        this.last = last;
    }

    /**
     * @return Last.
     */
    public GridH2RowMessage last() {
        return last;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -35;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2RowRangeBounds.class, this);
    }
}

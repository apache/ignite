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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.DeferredUnmarshalMessage;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.marshaller.Marshaller;

/** */
public class QueryBatchMessage implements MarshallableMessage, DeferredUnmarshalMessage, ExecutionContextAware {
    /** */
    @Order(0)
    UUID qryId;

    /** */
    @Order(1)
    long fragmentId;

    /** */
    @Order(2)
    long exchangeId;

    /** */
    @Order(3)
    int batchId;

    /** */
    @Order(4)
    boolean last;

    /** */
    private List<Object> rows;

    /** */
    @Order(5)
    List<GenericValueMessage> mRows;

    /** */
    public QueryBatchMessage() {
        // No-op.
    }

    /** */
    public QueryBatchMessage(UUID qryId, long fragmentId, long exchangeId, int batchId, boolean last, List<Object> rows) {
        this.qryId = qryId;
        this.fragmentId = fragmentId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.last = last;
        this.rows = rows;
    }

    /** {@inheritDoc} */
    @Override public UUID queryId() {
        return qryId;
    }

    /** {@inheritDoc} */
    @Override public long fragmentId() {
        return fragmentId;
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * @return Batch ID.
     */
    public int batchId() {
        return batchId;
    }

    /**
     * @return Last batch flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Rows.
     */
    public List<Object> rows() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void marshal(Marshaller marsh) {
        if (mRows != null || rows == null)
            return;

        mRows = new ArrayList<>(rows.size());

        for (Object row : rows)
            mRows.add(row == null ? null : new GenericValueMessage(row));
    }

    /** {@inheritDoc} */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (rows == null && mRows != null) {
            rows = new ArrayList<>(mRows.size());

            for (GenericValueMessage mRow : mRows)
                rows.add(mRow == null ? null : mRow.value());
        }

        mRows = null;
    }
}

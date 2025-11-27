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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/**
 *
 */
public class QueryBatchMessage implements MarshalableMessage, ExecutionContextAware {
    /** */
    @Order(value = 0, method = "queryId")
    private UUID qryId;

    /** */
    @Order(1)
    private long fragmentId;

    /** */
    @Order(2)
    private long exchangeId;

    /** */
    @Order(3)
    private int batchId;

    /** */
    @Order(4)
    private boolean last;

    /** */
    private List<Object> rows;

    /** */
    @Order(value = 5, method = "messageRows")
    private List<ValueMessage> mRows;

    /** */
    public QueryBatchMessage() {
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

    /**
     * @param qryId Query ID.
     */
    public void queryId(UUID qryId) {
        this.qryId = qryId;
    }

    /** {@inheritDoc} */
    @Override public long fragmentId() {
        return fragmentId;
    }

    /**
     * @param fragmentId Fragment ID.
     */
    public void fragmentId(long fragmentId) {
        this.fragmentId = fragmentId;
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * @param exchangeId Exchange ID.
     */
    public void exchangeId(long exchangeId) {
        this.exchangeId = exchangeId;
    }

    /**
     * @return Batch ID.
     */
    public int batchId() {
        return batchId;
    }

    /**
     * @param batchId Batch ID.
     */
    public void batchId(int batchId) {
        this.batchId = batchId;
    }

    /**
     * @return Last batch flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @param last Last batch flag.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /**
     * @return Rows.
     */
    public List<Object> rows() {
        return rows;
    }

    /**
     * @return Message rows.
     */
    public List<ValueMessage> messageRows() {
        return mRows;
    }

    /**
     * @param mRows Message rows.
     */
    public void messageRows(List<ValueMessage> mRows) {
        this.mRows = mRows;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (mRows != null || rows == null)
            return;

        mRows = new ArrayList<>(rows.size());

        for (Object row : rows) {
            ValueMessage mRow = CalciteMessageFactory.asMessage(row);

            assert mRow != null;

            mRow.prepareMarshal(ctx);

            mRows.add(mRow);
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (rows != null || mRows == null)
            return;

        rows = new ArrayList<>(mRows.size());

        for (ValueMessage mRow : mRows) {
            assert mRow != null;

            mRow.prepareUnmarshal(ctx);

            rows.add(mRow.value());
        }
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_BATCH_MESSAGE;
    }
}

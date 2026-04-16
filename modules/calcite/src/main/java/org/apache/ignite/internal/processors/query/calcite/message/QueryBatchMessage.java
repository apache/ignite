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

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Order;

/** */
public class QueryBatchMessage implements ExecutionContextAware {
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

        mRows = rows.stream().map(o -> o == null ? null : new GenericValueMessage(o)).collect(Collectors.toList());
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
        return mRows.stream().map(GenericValueMessage::value).collect(Collectors.toList());
    }
}

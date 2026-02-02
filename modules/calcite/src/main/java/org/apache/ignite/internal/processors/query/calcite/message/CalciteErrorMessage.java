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

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;

/**
 *
 */
public class CalciteErrorMessage extends ErrorMessage implements CalciteMessage {
    /** */
    @Order(value = 1, method = "queryId")
    private UUID qryId;

    /** */
    @Order(2)
    private long fragmentId;

    /** */
    public CalciteErrorMessage() {
        // No-op.
    }

    /** */
    public CalciteErrorMessage(UUID qryId, long fragmentId, Throwable err) {
        super(err);

        assert err != null;

        this.qryId = qryId;
        this.fragmentId = fragmentId;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /** */
    public void queryId(UUID qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /** */
    public void fragmentId(long fragmentId) {
        this.fragmentId = fragmentId;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_ERROR_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return MessageType.QUERY_ERROR_MESSAGE.directType();
    }
}

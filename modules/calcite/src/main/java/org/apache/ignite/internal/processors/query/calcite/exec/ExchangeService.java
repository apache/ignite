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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.util.Service;

/**
 *
 */
public interface ExchangeService extends Service {
    /**
     * Sends a batch of data to remote node.
     * @param nodeId Target node ID.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     * @param last Last batch flag.
     * @param rows Data rows.
     */
    <Row> void sendBatch(UUID nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId, boolean last,
        List<Row> rows) throws IgniteCheckedException;

    /**
     * Acknowledges a batch with given ID is processed.
     * @param nodeId Node ID to notify.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     */
    void acknowledge(UUID nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId) throws IgniteCheckedException;

    /**
     * Sends cancel request.
     * @param nodeId Target node ID.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     */
    void cancel(UUID nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId) throws IgniteCheckedException;

    /**
     * @param nodeId Target node ID.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param err Exception to send.
     * @throws IgniteCheckedException On error marshaling or send ErrorMessage.
     */
    void sendError(
        UUID nodeId,
        UUID qryId,
        long fragmentId,
        long exchangeId,
        Throwable err
    ) throws IgniteCheckedException;
}

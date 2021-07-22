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

import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 *
 */
public interface ExchangeService {
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
    <Row> void sendBatch(String nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId, boolean last,
        List<Row> rows) throws IgniteInternalCheckedException;

    /**
     * Acknowledges a batch with given ID is processed.
     * @param nodeId Node ID to notify.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     */
    void acknowledge(String nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId) throws IgniteInternalCheckedException;

    /**
     * Sends cancel request.
     * @param nodeId Target node ID.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     */
    void closeInbox(String nodeId, UUID qryId, long fragmentId, long exchangeId) throws IgniteInternalCheckedException;

    /**
     * Sends cancel request.
     * @param nodeId Target node ID.
     * @param qryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     */
    void closeOutbox(String nodeId, UUID qryId, long fragmentId, long exchangeId) throws IgniteInternalCheckedException;

    /**
     * @param nodeId Target node ID.
     * @param qryId Query ID.
     * @param fragmentId Source fragment ID.
     * @param err Exception to send.
     * @throws IgniteInternalCheckedException On error marshaling or send ErrorMessage.
     */
    void sendError(String nodeId, UUID qryId, long fragmentId, Throwable err) throws IgniteInternalCheckedException;

    /**
     * @param nodeId Node ID.
     * @return {@code true} if node is alive, {@code false} otherwise.
     */
    boolean alive(String nodeId);
}

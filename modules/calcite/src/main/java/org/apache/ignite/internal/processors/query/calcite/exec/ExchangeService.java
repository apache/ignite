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

/**
 *
 */
public interface ExchangeService {
    /** A number of rows in a single batch. */
    int BATCH_SIZE = 200;

    /** A maximum allowed unprocessed batches count per node. */
    int PER_NODE_BATCH_COUNT = 10;

    /**
     * Sends a batch of data to remote node.
     *
     * @param caller Caller.
     * @param nodeId Target node ID.
     * @param queryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     * @param rows Data rows.
     */
    void sendBatch(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows);

    /**
     * Acknowledges a batch with given ID is processed.
     *
     * @param caller Caller.
     * @param nodeId Node ID to notify.
     * @param queryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     */
    void acknowledge(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId);

    /**
     * Sends cancel request.
     *
     * @param caller Caller.
     * @param nodeId Target node ID.
     * @param queryId Query ID.
     * @param fragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param batchId Batch ID.
     */
    void cancel(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId);
}

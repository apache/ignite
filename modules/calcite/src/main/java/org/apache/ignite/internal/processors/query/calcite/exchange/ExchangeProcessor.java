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

package org.apache.ignite.internal.processors.query.calcite.exchange;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public interface ExchangeProcessor {
    /** A number of rows in a single batch. */
    int BATCH_SIZE = 200;

    /** A maximum allowed unprocessed batches count per node. */
    int PER_NODE_BATCH_COUNT = 10;

    /**
     * Registers an outbox in Exchange service.
     * Registered outbox will be notified each time a consumer processed a batch of rows.
     * @param outbox Outbox.
     * @return Registered outbox.
     */
    <T> Outbox<T> register(Outbox<T> outbox);

    /**
     * Unregisters an outbox in Exchange service.
     * @param outbox Outbox to unregister.
     */
    <T> void unregister(Outbox<T> outbox);

    /**
     * Registers an inbox in Exchange service.
     * Registered inbox starts consuming data from remote sources.
     * In case an inbox with the same [queryId, exchangeId] pair is already registered, previously registered inbox is return.
     * @param inbox Inbox.
     * @return Registered inbox.
     */
    <T> Inbox<T> register(Inbox<T> inbox);

    /**
     * Unregisters an inbox in Exchange service.
     * @param inbox Inbox to unregister.
     */
    <T> void unregister(Inbox<T> inbox);

    /**
     * Sends a batch of data to remote node.
     * @param queryId Query ID.
     * @param exchangeId Exchange ID.
     * @param nodeId Target node ID.
     * @param batchId Batch ID.
     * @param rows Data rows.
     */
    void send(GridCacheVersion queryId, long exchangeId, UUID nodeId, int batchId, List<?> rows);

    /**
     * Acknowledges a batch with given ID is processed.
     * @param queryId Query ID.
     * @param exchangeId Exchange ID.
     * @param nodeId Node ID to notify.
     * @param batchId Batch ID.
     */
    void acknowledge(GridCacheVersion queryId, long exchangeId, UUID nodeId, int batchId);
}

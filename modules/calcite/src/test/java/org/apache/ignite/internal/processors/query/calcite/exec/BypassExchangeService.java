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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;

/** */
public class BypassExchangeService implements ExchangeService, MailboxRegistry {
    /** */
    private final ConcurrentHashMap<Key, Outbox<?>> outboxes = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentHashMap<Key, Inbox<?>> inboxes = new ConcurrentHashMap<>();

    /** */
    private final Map<UUID, ExecutorService> executors;

    /** */
    private final IgniteLogger log;

    /**
     * @param log Logger.
     */
    public BypassExchangeService(Map<UUID, ExecutorService> executors, IgniteLogger log) {
        this.executors = executors;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> register(Inbox<?> inbox) {
        UUID nodeId = inbox.context().parent().localNodeId();
        UUID queryId = inbox.queryId();
        long exchangeId = inbox.exchangeId();

        Inbox<?> old = inboxes.putIfAbsent(new Key(nodeId, queryId, exchangeId), inbox);

        return old != null ? old : inbox;
    }

    /** {@inheritDoc} */
    @Override public void unregister(Inbox<?> inbox) {
        UUID nodeId = inbox.context().parent().localNodeId();
        UUID queryId = inbox.queryId();
        long exchangeId = inbox.exchangeId();

        inboxes.remove(new Key(nodeId, queryId, exchangeId));
    }

    @Override public void register(Outbox<?> outbox) {
        UUID nodeId = outbox.context().parent().localNodeId();
        UUID queryId = outbox.queryId();
        long exchangeId = outbox.exchangeId();

        outboxes.put(new Key(nodeId, queryId, exchangeId), outbox);
    }

    @Override public void unregister(Outbox<?> outbox) {
        UUID nodeId = outbox.context().parent().localNodeId();
        UUID queryId = outbox.queryId();
        long exchangeId = outbox.exchangeId();

        outboxes.remove(new Key(nodeId, queryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public Outbox<?> outbox(UUID queryId, long exchangeId) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> inbox(UUID queryId, long exchangeId) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public void sendBatch(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows) {
        Inbox<?> inbox = inboxes.computeIfAbsent(new Key(nodeId, queryId, exchangeId), k -> newInbox(nodeId, queryId, fragmentId, exchangeId, batchId));

        if (inbox != null) {
            UUID senderNodeId = ((Node<?>) caller).context().parent().localNodeId();

            inbox.context().execute(() -> inbox.onBatchReceived(senderNodeId, batchId, rows));
        }
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        Outbox<?> outbox = outboxes.get(new Key(nodeId, queryId, exchangeId));

        if (outbox != null) {
            UUID senderNodeId = ((Node<?>)caller).context().parent().localNodeId();

            outbox.context().execute(() -> outbox.onAcknowledge(senderNodeId, batchId));
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        inboxes.remove(new Key(nodeId, queryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public void onCancel(Object caller, UUID nodeId, UUID queryId, long exchangeId, long fragmentId, int batchId) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public void onAcknowledge(Object caller, UUID nodeId, UUID queryId, long exchangeId, long fragmentId, int batchId) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public void onBatchReceived(Object caller, UUID nodeId, UUID queryId, long exchangeId, long fragmentId, int batchId, List<?> rows) {
        throw new AssertionError();
    }

    /** */
    private Inbox<?> newInbox(UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        if (batchId != 0)
            return null; // stale message

        ExecutorService exec = executors.computeIfAbsent(nodeId, id -> Executors.newSingleThreadExecutor());

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(nodeId)
            .exchangeService(this)
            .taskExecutor((qid, fid, t) -> CompletableFuture.runAsync(t, exec).exceptionally(this::handle))
            .logger(log)
            .build();

        ExecutionContext ectx = new ExecutionContext(
            ctx,
            queryId,
            fragmentId,
            null,
            ImmutableMap.of());

        // exchange ID is the same as source fragment ID
        return new Inbox<>(ectx, exchangeId, exchangeId);
    }

    /** */
    private Void handle(Throwable ex) {
        log.error(ex.getMessage(), ex);
        return null;
    }

    /** */
    private static class Key {
        /** */
        private final UUID nodeId;

        /** */
        private final UUID queryId;

        /** */
        private final long exchangeId;

        /** */
        private Key(UUID nodeId, UUID queryId, long exchangeId) {
            this.nodeId = nodeId;
            this.queryId = queryId;
            this.exchangeId = exchangeId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key) o;

            if (exchangeId != key.exchangeId)
                return false;
            if (!nodeId.equals(key.nodeId))
                return false;
            return queryId.equals(key.queryId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = nodeId.hashCode();
            result = 31 * result + queryId.hashCode();
            result = 31 * result + (int) (exchangeId ^ (exchangeId >>> 32));
            return result;
        }
    }
}

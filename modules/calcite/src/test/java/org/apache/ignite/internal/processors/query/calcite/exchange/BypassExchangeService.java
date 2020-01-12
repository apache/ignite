/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exchange;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.Outbox;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.splitter.Fragment;

/** */
public class BypassExchangeService implements ExchangeService, MailboxRegistry {
    /** */
    private final ConcurrentHashMap<Key, Outbox<?>> outboxes = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentHashMap<Key, Inbox<?>> inboxes = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /**
     * @param log Logger.
     */
    public BypassExchangeService(IgniteLogger log) {
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
    @Override public void sendBatch(Outbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId, List<?> rows) {
        Inbox<?> inbox = inboxes.computeIfAbsent(new Key(nodeId, queryId, exchangeId), this::newInbox);

        UUID senderNode = sender.context().parent().localNodeId();

        inbox.push(senderNode, batchId, rows);
    }

    /** {@inheritDoc} */
    @Override public void sendAcknowledgment(Inbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId) {
        Outbox<?> outbox = outboxes.get(new Key(nodeId, queryId, exchangeId));

        if (outbox != null) {
            UUID senderNode = sender.context().parent().localNodeId();

            outbox.onAcknowledge(senderNode, batchId);
        }
    }

    /** {@inheritDoc} */
    @Override public void sendCancel(Outbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId) {
        inboxes.remove(new Key(nodeId, queryId, exchangeId));
    }

    /** */
    private Inbox<?> newInbox(Key k) {
        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(k.nodeId)
            .exchangeService(this)
            .taskExecutor((qid, fid, t) -> CompletableFuture.completedFuture(null))
            .logger(log)
            .build();

        return new Inbox<>(new ExecutionContext(ctx, k.queryId, Fragment.UNDEFINED_ID, null, ImmutableMap.of()), k.exchangeId);
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

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
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.Outbox;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;

/** */
public class BypassExchangeProcessor implements ExchangeProcessor {
    /** */
    private final ConcurrentHashMap<Pair<UUID, Long>, Outbox<?>> outboxes = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentHashMap<Pair<UUID, Long>, Inbox<?>> inboxes = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /**
     * @param log Logger.
     */
    public BypassExchangeProcessor(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void register(Outbox<?> outbox) {
        outboxes.put(Pair.of(outbox.queryId(), outbox.exchangeId()), outbox);
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> register(Inbox<?> inbox) {
        Inbox<?> old = inboxes.putIfAbsent(Pair.of(inbox.queryId(), inbox.exchangeId()), inbox);

        return old != null ? old : inbox;
    }

    /** {@inheritDoc} */
    @Override public void unregister(Outbox<?> outbox) {
        outboxes.remove(Pair.of(outbox.queryId(), outbox.exchangeId()));
    }

    /** {@inheritDoc} */
    @Override public void unregister(Inbox<?> inbox) {
        inboxes.remove(Pair.of(inbox.queryId(), inbox.exchangeId()));
    }

    /** {@inheritDoc} */
    @Override public void send(UUID queryId, long exchangeId, UUID nodeId, int batchId, List<?> rows) {
        inboxes.computeIfAbsent(Pair.of(queryId, exchangeId), this::newInbox).push(nodeId, batchId, rows);
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(UUID queryId, long exchangeId, UUID nodeId, int batchId) {
        Outbox<?> outbox = outboxes.get(Pair.of(queryId, exchangeId));

        if (outbox != null)
            outbox.onAcknowledge(nodeId, batchId);
    }

    /** */
    private Inbox<?> newInbox(Pair<UUID, Long> k) {
        PlannerContext ctx = PlannerContext.builder()
            .exchangeProcessor(this)
            .executor((t,i) -> CompletableFuture.completedFuture(null))
            .logger(log)
            .build();

        return new Inbox<>(new ExecutionContext(k.left, ctx, ImmutableMap.of()), k.right);
    }
}

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

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.Outbox;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.QueryAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryInboxCancelMessage;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;

/**
 *
 */
public class ExchangeServiceImpl implements ExchangeService, LifecycleAware {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private MessageService messageService;

    public ExchangeServiceImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(ExchangeServiceImpl.class);
    }


    @Override public void sendBatch(Outbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId, List<?> rows) {
        messageService.send(nodeId, new QueryBatchMessage(queryId, exchangeId, batchId, rows));
    }

    @Override public void sendAcknowledgment(Inbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId) {
        messageService.send(nodeId, new QueryAcknowledgeMessage(queryId, exchangeId, batchId));
    }

    @Override public void sendCancel(Outbox<?> sender, UUID nodeId, UUID queryId, long exchangeId, int batchId) {
        messageService.send(nodeId, new QueryInboxCancelMessage(queryId, exchangeId, batchId));
    }

    @Override public void onStart(GridKernalContext ctx) {
        messageService =
            Objects.requireNonNull(Commons.lookup(ctx, CalciteQueryProcessor.class)).messageService();
    }

    @Override public void onStop() {
        messageService = null;
    }
}

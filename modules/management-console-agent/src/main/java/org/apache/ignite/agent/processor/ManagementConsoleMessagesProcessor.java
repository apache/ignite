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

package org.apache.ignite.agent.processor;

import java.util.UUID;
import org.apache.ignite.agent.dto.NodeConfiguration;
import org.apache.ignite.agent.dto.tracing.SpanBatch;
import org.apache.ignite.agent.ws.RetryableSender;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.agent.ManagementConsoleProcessor.TOPIC_MANAGEMENT_CONSOLE;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterNodeConfigurationDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildEventsDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.apache.ignite.agent.utils.AgentUtils.quiteStop;

/**
 * Management console messages processor.
 */
public class ManagementConsoleMessagesProcessor extends GridProcessorAdapter {
    /** Sender. */
    private final RetryableSender snd;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, Object> lsnr = this::processMessage;

    /**
     * @param ctx Context.
     */
    public ManagementConsoleMessagesProcessor(GridKernalContext ctx) {
        super(ctx);

        this.snd = new RetryableSender(ctx);

        ctx.grid().message().localListen(TOPIC_MANAGEMENT_CONSOLE, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.grid().message().stopLocalListen(TOPIC_MANAGEMENT_CONSOLE, lsnr);

        quiteStop(snd);
    }

    /**
     * @param nid UUID.
     * @param msg Message.
     */
    boolean processMessage(UUID nid, Object msg) {
        if (msg instanceof NodeConfiguration)
            snd.send(buildClusterNodeConfigurationDest(ctx.cluster().get().id()), msg);
        else if (msg instanceof VisorGridEvent)
            snd.send(buildEventsDest(ctx.cluster().get().id()), msg);
        else if (msg instanceof SpanBatch)
            snd.sendList(buildSaveSpanDest(ctx.cluster().get().id()), ((SpanBatch)msg).list());

        return true;
    }
}

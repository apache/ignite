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

package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;

/**
 * Tracing sub-system implementation.
 */
public class TracingProcessor extends GridProcessorAdapter implements Tracing {
    /** Spi. */
    private TracingSpi spi;

    /** Traceable messages handler. */
    private final TraceableMessagesHandler msgHnd;

    /**
     * @param ctx Kernal context.
     */
    public TracingProcessor(GridKernalContext ctx) {
        super(ctx);

        spi = ctx.config().getTracingSpi();

        msgHnd = new TraceableMessagesHandler(this, ctx.log(TracingProcessor.class));
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        try {
            ctx.resource().inject(spi);

            spi.spiStart(ctx.igniteInstanceName());
        }
        catch (IgniteSpiException e) {
            log.warning("Failed to start tracing processor with spi: " + spi.getName()
                + ". Noop implementation will be used instead.", e);

            spi = new NoopTracingSpi();

            spi.spiStart(ctx.igniteInstanceName());
        }

        if (log.isInfoEnabled())
            log.info("Started tracing processor with configured spi: " + spi.getName());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        spi.spiStop();
    }

    /**
     * Adds tags with information about local node to given {@code span}.
     *
     * @param span Span.
     * @return Span enriched by local node information.
     */
    private Span enrichWithLocalNodeParameters(Span span) {
        span.addTag(SpanTags.NODE_ID, ctx.localNodeId().toString());
        span.addTag(SpanTags.tag(NODE, SpanTags.NAME), ctx.igniteInstanceName());

        ClusterNode localNode = ctx.discovery().localNode();
        if (localNode != null && localNode.consistentId() != null)
            span.addTag(SpanTags.tag(NODE, SpanTags.CONSISTENT_ID), localNode.consistentId().toString());

        return span;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        return enrichWithLocalNodeParameters(spi.create(name, parentSpan));
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        return enrichWithLocalNodeParameters(spi.create(name, serializedSpanBytes));
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        return spi.serialize(span);
    }

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        return msgHnd;
    }
}

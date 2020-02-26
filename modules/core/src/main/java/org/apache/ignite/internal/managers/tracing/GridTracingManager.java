/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.tracing;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.tracing.NoopTracingSpi;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;

/**
 * Tracing Manager.
 */
public class GridTracingManager extends GridManagerAdapter<TracingSpi> implements Tracing {

    /** Traceable messages handler. */
    private final TraceableMessagesHandler msgHnd;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param useNoopTracingSpi Flag that signals that NoOp tracing spi should be used instead of the one,
     * specified in the context. It's a part of the failover logic that is suitable if an exception is thrown
     * when the manager starts.
     */
    public GridTracingManager(GridKernalContext ctx, boolean useNoopTracingSpi) {
        super(ctx, useNoopTracingSpi ? new NoopTracingSpi() : ctx.config().getTracingSpi());

        msgHnd = new TraceableMessagesHandler(this, ctx.log(GridTracingManager.class));
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void start() throws IgniteCheckedException {
        try {
            startSpi();
        }
        catch (IgniteSpiException e) {
            log.warning("Failed to start tracing processor with spi: " + getSpi().getName()
                + ". Noop implementation will be used instead.", e);

            throw e;
        }

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
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

        ClusterNode locNode = ctx.discovery().localNode();
        if (locNode != null && locNode.consistentId() != null)
            span.addTag(SpanTags.tag(NODE, SpanTags.CONSISTENT_ID), locNode.consistentId().toString());

        return span;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        return enrichWithLocalNodeParameters(getSpi().create(name, parentSpan));
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        return enrichWithLocalNodeParameters(getSpi().create(name, serializedSpanBytes));
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        return getSpi().serialize(span);
    }

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        return msgHnd;
    }
}
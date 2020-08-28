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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.processors.tracing.messages.SpanTransport;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_SOCKET_READ;

/**
 * Filter that inject and extract tracing span from/to process.
 */
public class GridNioTracerFilter extends GridNioFilterAdapter {
    /** Grid logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /** Tracing processor. */
    private final Tracing tracer;

    /**
     * Creates a tracer filter.
     *
     * @param log Log instance to use.
     * @param tracer Tracing processor.
     */
    public GridNioTracerFilter(IgniteLogger log, Tracing tracer) {
        super("GridNioTracerFilter");

        this.log = log;
        this.tracer = tracer == null ? new NoopTracing() : tracer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioTracerFilter.class, this);
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(
        GridNioSession ses,
        IgniteCheckedException ex
    ) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        if (msg instanceof SpanTransport && MTC.span() != NoopSpan.INSTANCE)
            ((SpanTransport)msg).span(tracer.serialize(MTC.span()));

        return proceedSessionWrite(ses, msg, fut, ackC);

    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        byte[] serializedSpan = msg instanceof SpanTransport ? ((SpanTransport)msg).span() : null;

        if (serializedSpan != null && serializedSpan.length != 0) {
            Span span = tracer.create(COMMUNICATION_SOCKET_READ, serializedSpan);

            try (MTC.TraceSurroundings ignore = MTC.support(span)) {
                proceedMessageReceived(ses, msg);
            }
        }
        else
            proceedMessageReceived(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }
}

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
package org.apache.ignite.internal.processors.tracing.messages;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.SpanManager;

/**
 * Helper to handle traceable messages.
 */
public class TraceableMessagesHandler {
    /** Span manager. */
    private final SpanManager spanMgr;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param spanMgr Span manager.
     * @param log Logger.
     */
    public TraceableMessagesHandler(SpanManager spanMgr, IgniteLogger log) {
        this.spanMgr = spanMgr;
        this.log = log;
    }

    /**
     * Called when message is received.
     * A span with name associated with given message will be created.
     * from contained serialized span {@link SpanContainer#serializedSpanBytes()}
     *
     * @param msg Traceable message.
     */
    public void afterReceive(TraceableMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Received traceable message: " + msg);

        if (msg.spanContainer().span() == NoopSpan.INSTANCE && msg.spanContainer().serializedSpanBytes() != null)
            msg.spanContainer().span(
                spanMgr.create(TraceableMessagesTable.traceName(msg.getClass()), msg.spanContainer().serializedSpanBytes())
                    .addLog(() -> "Received")
            );
    }

    /**
     * Called when message is going to be send.
     * A serialized span will be created and attached to {@link TraceableMessage#spanContainer()}.
     *
     * @param msg Traceable message.
     */
    public void beforeSend(TraceableMessage msg) {
        if (msg.spanContainer().span() != NoopSpan.INSTANCE && msg.spanContainer().serializedSpanBytes() == null)
            msg.spanContainer().serializedSpanBytes(spanMgr.serialize(msg.spanContainer().span()));
    }

    /**
     * Injects a sub-span to {@code msg} as child span contained in given {@code parent}.
     *
     * @param msg Branched message.
     * @param parent Parent message.
     * @param <T> Traceable message type.
     * @return Branched message with span context from parent message.
     */
    public <T extends TraceableMessage> T branch(T msg, TraceableMessage parent) {
        assert parent.spanContainer().span() != null : parent;

        msg.spanContainer().serializedSpanBytes(
            spanMgr.serialize(parent.spanContainer().span())
        );

        msg.spanContainer().span(
            spanMgr.create(TraceableMessagesTable.traceName(msg.getClass()), parent.spanContainer().span())
                .addLog(() -> "Created")
        );

        return msg;
    }

    /**
     * @param msg Message.
     */
    public void finishProcessing(TraceableMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Processed traceable message: " + msg);

        if (!msg.spanContainer().span().isEnded())
            msg.spanContainer().span()
                .addLog(() -> "Processed")
                .end();
    }
}

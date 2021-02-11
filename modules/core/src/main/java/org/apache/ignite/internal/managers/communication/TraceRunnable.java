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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.Tracing;

/**
 * Wrapper of {@link Runnable} which incject tracing to execution.
 */
public abstract class TraceRunnable implements Runnable {
    /** */
    private final Tracing tracing;

    /** SpanType of the new span. */
    private final SpanType spanType;

    /** Parent span from which new span should be created. */
    private final Span parent;

    /**
     * @param tracing Tracing processor.
     * @param spanType Span type to create.
     */
    protected TraceRunnable(Tracing tracing, SpanType spanType) {
        this.tracing = tracing;
        this.spanType = spanType;
        parent = MTC.span();
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Span span = tracing.create(spanType, parent);

        try (TraceSurroundings ignore = MTC.support(span.equals(NoopSpan.INSTANCE) ? parent : span)) {
            execute();
        }
    }

    /**
     * Main code to execution.
     */
    public abstract void execute();
}

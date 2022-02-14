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
package org.apache.ignite.internal.processors.tracing;

import org.jetbrains.annotations.NotNull;

/**
 * Mapped tracing context.
 *
 * Thread local context which holding the information for tracing.
 */
public class MTC {
    /***/
    private static final Span NOOP_SPAN = NoopSpan.INSTANCE;

    /** Thread local span holder. */
    private static ThreadLocal<Span> span = ThreadLocal.withInitial(() -> NOOP_SPAN);

    /**
     * @return Span which corresponded to current thread or null if it doesn't not set.
     */
    @NotNull public static Span span() {
        return span.get();
    }

    /**
     * Attach given span to current thread if it isn't null or NOOP_SPAN. Detach given span, close
     * it and return previous span when {@link TraceSurroundings#close()} would be called.
     *
     * @param startSpan Span which should be added to current thread.
     * @return {@link TraceSurroundings} for manage span life cycle or null in case of null or no-op span.
     */
    public static TraceSurroundings support(Span startSpan) {
        if (startSpan == null || startSpan == NOOP_SPAN)
            return null;

        Span oldSpan = span();

        span.set(startSpan);

        return new TraceSurroundings(oldSpan, true);
    }

    /**
     * Support initial span.
     *
     * @param startSpan Span which should be added to current thread.
     */
    public static void supportInitial(Span startSpan) {
        span.set(startSpan);
    }

    /**
     * Attach given span to current thread if it isn't null or NOOP_SPAN.
     *
     * @param startSpan Span which should be added to current thread.
     * @return {@link TraceSurroundings} for manage span life cycle or null in case of null or no-op span.
     */
    public static TraceSurroundings supportContinual(Span startSpan) {
        if (startSpan == null || startSpan == NOOP_SPAN)
            return null;

        Span oldSpan = span();

        span.set(startSpan);

        return new TraceSurroundings(oldSpan, false);
    }

    /**
     * Helper for managing of span life cycle. It help to end current span and also reattach previous one to thread after
     * {@link TraceSurroundings#close()} would be call.
     */
    public static class TraceSurroundings implements AutoCloseable {
        /** Span which should be attached to thread when {@code #close} would be called. */
        private final Span oldSpan;

        /** {@code true} if current span should be ended at close moment. */
        private final boolean endRequired;

        /**
         * @param oldSpan Old span for restoring after close.
         * @param endRequired {@code true} if current span should be ended at close moment.
         */
        private TraceSurroundings(Span oldSpan, boolean endRequired) {
            this.oldSpan = oldSpan;
            this.endRequired = endRequired;
        }

        /**
         * Close life cycle of current span.
         */
        @Override public void close() {
            if (endRequired)
                span.get().end();

            span.set(oldSpan);
        }
    }
}

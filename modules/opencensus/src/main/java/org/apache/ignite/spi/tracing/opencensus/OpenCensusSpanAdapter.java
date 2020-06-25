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

package org.apache.ignite.spi.tracing.opencensus;

import io.opencensus.trace.AttributeValue;
import org.apache.ignite.spi.tracing.SpanStatus;
import org.apache.ignite.spi.tracing.SpiSpecificSpan;

/**
 * Span implementation based on OpenCensus library.
 */
public class OpenCensusSpanAdapter implements SpiSpecificSpan {
    /** OpenCensus span delegate. */
    private final io.opencensus.trace.Span span;

    /** Flag indicates that span is ended. */
    private volatile boolean ended;

    /**
     * @param span OpenCensus span delegate.
     */
    OpenCensusSpanAdapter(io.opencensus.trace.Span span) {
        this.span = span;
    }

    /** Implementation object. */
    public io.opencensus.trace.Span impl() {
        return span;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter addTag(String tagName, String tagVal) {
        tagVal = tagVal != null ? tagVal : "null";

        span.putAttribute(tagName, AttributeValue.stringAttributeValue(tagVal));

        return this;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter addLog(String logDesc) {
        span.addAnnotation(logDesc);

        return this;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter setStatus(SpanStatus spanStatus) {
        span.setStatus(StatusMatchTable.match(spanStatus));

        return this;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter end() {
        span.end();

        ended = true;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return ended;
    }
}

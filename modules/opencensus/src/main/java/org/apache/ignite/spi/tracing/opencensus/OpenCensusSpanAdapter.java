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

package org.apache.ignite.spi.tracing.opencensus;

import java.util.Map;
import java.util.stream.Collectors;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import org.apache.ignite.internal.processors.tracing.SpanStatus;
import org.apache.ignite.internal.processors.tracing.SpiSpecificSpan;
import org.apache.ignite.opencensus.spi.tracing.StatusMatchTable;

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
    @Override public SpiSpecificSpan addTag(String tagName, long tagVal) {
        span.putAttribute(tagName, AttributeValue.longAttributeValue(tagVal));

        return this;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter addLog(String logDesc) {
        span.addAnnotation(logDesc);

        return this;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter addLog(String logDesc, Map<String, String> attrs) {
        span.addAnnotation(Annotation.fromDescriptionAndAttributes(
            logDesc,
            attrs.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> AttributeValue.stringAttributeValue(e.getValue())
                ))
        ));

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

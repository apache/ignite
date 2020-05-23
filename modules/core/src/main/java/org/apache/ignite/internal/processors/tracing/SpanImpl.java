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

import java.util.Map;
import java.util.Set;

/**
 * Implementation of a {@link Span}
 */
public class SpanImpl implements Span {
    /** Spi specific span delegate. */
    private final SpiSpecificSpan spiSpecificSpan;

    /** Span type. */
    private final SpanType spanType;

    /** Set of extra included scopes for given span in addition to span's scope that is supported by default. */
    private final Set<Scope> includedScopes;

    /**
     * Constructor
     *
     * @param spiSpecificSpan Spi specific span.
     * @param spanType Type of a span.
     * @param includedScopes Set of included scopes.
     */
    public SpanImpl(
        SpiSpecificSpan spiSpecificSpan,
        SpanType spanType,
        Set<Scope> includedScopes) {
        this.spiSpecificSpan = spiSpecificSpan;
        this.spanType = spanType;
        this.includedScopes = includedScopes;
    }

    /** {@inheritDoc} */
    @Override public Span addTag(String tagName, String tagVal) {
        spiSpecificSpan.addTag(tagName, tagVal);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addTag(String tagName, long tagVal) {
        spiSpecificSpan.addTag(tagName, tagVal);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addLog(String logDesc) {
        spiSpecificSpan.addLog(logDesc);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addLog(String logDesc, Map<String, String> attrs) {
        spiSpecificSpan.addLog(logDesc, attrs);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span setStatus(SpanStatus spanStatus) {
        spiSpecificSpan.setStatus(spanStatus);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span end() {
        spiSpecificSpan.end();

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return spiSpecificSpan.isEnded();
    }

    /** {@inheritDoc} */
    @Override public SpanType type() {
        return spanType;
    }

    /** {@inheritDoc} */
    @Override public Set<Scope> includedScopes() {
        return includedScopes;
    }

    /**
     * @return Spi specific span delegate.
     */
    public SpiSpecificSpan spiSpecificSpan() {
        return spiSpecificSpan;
    }
}

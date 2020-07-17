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

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.SpanStatus;

/**
 * Encapsulates concept of a deferred-initialized span. It's used to overcome OpenCensus span implementation, that starts
 * span immediately after deserialization.
 */
public class DeferredSpan implements Span {
    /** */
    private byte[] serializedSpan;

    /**
     * Constructor.
     *
     * @param serializedSpan Serialized span bytes.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public DeferredSpan(byte[] serializedSpan) {
        this.serializedSpan = serializedSpan;
    }

    /**
     * @return Serialized span.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public byte[] serializedSpan() {
        return serializedSpan;
    }

    /** {@inheritDoc} */
    @Override public Span addTag(String tagName, Supplier<String> tagValSupplier) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addLog(Supplier<String> logDescSupplier) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span setStatus(SpanStatus spanStatus) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span end() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SpanType type() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Set<Scope> includedScopes() {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public boolean isChainable(Scope scope) {
        return false;
    }
}

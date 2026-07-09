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

import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.SpanStatus;

/**
 * Logical piece of a trace that represents a single operation.
 * Each unit work is called a Span in a trace.
 * Spans include metadata about the work, including the time spent in the step (latency),
 * status, time events, attributes, links.
 * You can use tracing to debug errors and latency issues in your applications.
 */
public interface Span {
    /**
     * Adds tag to span with {@code String} value.
     *
     * @param tagName Tag name.
     * @param tagValSupplier Tag value supplier. Supplier is used instead of strict tag value cause of it's lazy nature.
     *  So that it's possible not to generate String tag value in case of NoopSpan.
     */
    Span addTag(String tagName, Supplier<String> tagValSupplier);

    /**
     * Logs work to span.
     *
     * @param logDescSupplier Log description supplier.
     *  Supplier is used instead of strict log description cause of it's lazy nature.
     *  So that it's possible not to generate String log description in case of NoopSpan.
     */
    Span addLog(Supplier<String> logDescSupplier);

    /**
     * Explicitly set status for span.
     *
     * @param spanStatus Status.
     */
    Span setStatus(SpanStatus spanStatus);

    /**
     * Ends span. This action sets default status if not set and mark the span as ready to be exported.
     */
    Span end();

    /**
     * @return {@code true} if span has already ended.
     */
    boolean isEnded();

    /**
     * @return Type of given span.
     */
    SpanType type();

    /**
     * @return Set of included scopes.
     */
    Set<Scope> includedScopes();

    /**
     * @param scope Chainable scope candidate.
     * @return {@code true} if given span is chainable with other spans with specified scope.
     */
    default boolean isChainable(Scope scope) {
        return type().scope() == scope || includedScopes().contains(scope);
    }
}

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

package org.apache.ignite.internal.processors.tracing;

import java.util.Map;

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
     * @param tagVal Tag value.
     */
    public Span addTag(String tagName, String tagVal);
    /**
     * Adds tag to span with {@code long} value.
     *
     * @param tagName Tag name.
     * @param tagVal Tag value.
     */
    public Span addTag(String tagName, long tagVal);
    /**
     * Logs work to span.
     *
     * @param logDesc Log description.
     */
    public Span addLog(String logDesc);
    /**
     * Adds log to span with additional attributes.
     *
     * @param logDesc Log description.
     * @param attributes Attributes.
     */
    public Span addLog(String logDesc, Map<String, String> attributes);
    /**
     * Explicitly set status for span.
     *
     * @param spanStatus Status.
     */
    public Span setStatus(SpanStatus spanStatus);
    /**
     * Ends span. This action sets default status if not set and mark the span as ready to be exported.
     */
    public Span end();
    /**
     * @return {@code true} if span has already ended.
     */
    public boolean isEnded();
}

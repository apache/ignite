/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * Logical piece of a trace that insulates spi specific logic.
 */
public interface SpiSpecificSpan {

    /**
     * Adds tag to span with {@code String} value.
     *
     * @param tagName Tag name.
     * @param tagVal Tag value.
     */
    SpiSpecificSpan addTag(String tagName, String tagVal);

    /**
     * Adds tag to span with {@code long} value.
     *
     * @param tagName Tag name.
     * @param tagVal Tag value.
     */
    SpiSpecificSpan addTag(String tagName, long tagVal);

    /**
     * Logs work to span.
     *
     * @param logDesc Log description.
     */
    SpiSpecificSpan addLog(String logDesc);

    /**
     * Adds log to span with additional attributes.
     *
     * @param logDesc Log description.
     * @param attrs Attributes.
     */
    SpiSpecificSpan addLog(String logDesc, Map<String, String> attrs);

    /**
     * Explicitly set status for span.
     *
     * @param spanStatus Status.
     */
    SpiSpecificSpan setStatus(SpanStatus spanStatus);

    /**
     * Ends span. This action sets default status if not set and mark the span as ready to be exported.
     */
    SpiSpecificSpan end();

    /**
     * @return {@code true} if span has already ended.
     */
    boolean isEnded();
}

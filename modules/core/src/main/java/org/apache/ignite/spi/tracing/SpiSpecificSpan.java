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

package org.apache.ignite.spi.tracing;

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
     * Logs work to span.
     *
     * @param logDesc Log description.
     */
    SpiSpecificSpan addLog(String logDesc);

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

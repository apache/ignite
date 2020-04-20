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

package org.apache.ignite.internal.profiling.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/**
 * The interface represents log parser. Creates JSON for UI views.
 */
public interface IgniteLogParser {
    /**
     * Parse line from profiling log.
     *
     * @param nodeId Node id.
     * @param str String to parse.
     */
    void parse(String nodeId, String str);

    /** Callback on all logs parsed at first iteration. */
    default void onFirstPhaseEnd() {
        // No-op.
    }

    /**
     * Parse line from profiling log (second iteration).
     *
     * @param nodeId Node id.
     * @param str String to parse.
     */
    default void parsePhase2(String nodeId, String str) {
        // No-op.
    }

    /**
     * Map of named JSON results.
     *
     * @return Result map.
     */
    Map<String, JsonNode> results();
}

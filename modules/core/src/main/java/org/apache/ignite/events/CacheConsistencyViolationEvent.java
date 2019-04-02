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

package org.apache.ignite.events;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 * Event indicates consistency violation detection.
 *
 * @see EventType#EVT_CONSISTENCY_VIOLATION
 */
public class CacheConsistencyViolationEvent extends EventAdapter {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Map. */
    Map<UUID /*Node*/, Map<Object /*Key*/, Object /*Value*/>> map;

    /** Fixed map. */
    Map<Object /*Key*/, Object /*Value*/> fixedMap;

    /**
     * @param node Node.
     * @param msg Message.
     * @param map Map.
     * @param fixedMap Fixed map.
     */
    public CacheConsistencyViolationEvent(
        ClusterNode node,
        String msg,
        Map<UUID, Map<Object, Object>> map,
        Map<Object, Object> fixedMap) {
        super(node, msg, EVT_CONSISTENCY_VIOLATION);

        this.map = map;
        this.fixedMap = fixedMap;
    }

    /**
     * Detected distribution.
     */
    public Map<UUID, Map<Object, Object>> getMap() {
        return map;
    }

    /**
     * Proposed distribution.
     * Will be fixed in case of transaction commit.
     */
    public Map<Object, Object> getFixedMap() {
        return fixedMap;
    }
}
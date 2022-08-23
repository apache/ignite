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

package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 * Local object statistics change event.
 */
public class ObjectStatisticsEvent {
    /** Statistics key. */
    private final StatisticsKey key;

    /** Local object statistics. */
    private final ObjectStatisticsImpl statistics;

    /** Local statistics topology version. */
    private final AffinityTopologyVersion topVer;

    /**
     * Constructor.
     *
     * @param key Statistics key.
     * @param statistics Local object statistics.
     * @param topVer Affinity topology version for whick local object statistics were calculated.
     */
    public ObjectStatisticsEvent(StatisticsKey key,
        ObjectStatisticsImpl statistics, AffinityTopologyVersion topVer) {
        this.key = key;
        this.statistics = statistics;
        this.topVer = topVer;
    }

    /**
     * Get statistics key.
     *
     * @return Statistics key.
     */
    public StatisticsKey key() {
        return key;
    }

    /**
     * Get object statistics.
     *
     * @return Object statistics.
     */
    public ObjectStatisticsImpl statistics() {
        return statistics;
    }

    /**
     * Get affinity topology version.
     *
     * @return topology version for which statisics were calculated.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }
}

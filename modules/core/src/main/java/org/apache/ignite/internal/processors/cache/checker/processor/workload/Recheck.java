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

package org.apache.ignite.internal.processors.cache.checker.processor.workload;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Work container for recheck stage.
 */
public class Recheck extends PipelineWorkload {
    /** Recheck keys. */
    private final Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys;

    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partId;

    /** Serial number of recheck attempt. */
    private final int recheckAttempt;

    /** Serial number of repair attempt. */
    private final int repairAttempt;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param recheckKeys Recheck keys.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param recheckAttempt Recheck attempt.
     * @param repairAttempt Repair attempt.
     */
    public Recheck(
        long sesId,
        UUID workloadChainId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys,
        String cacheName,
        int partId,
        int recheckAttempt,
        int repairAttempt
    ) {
        super(sesId, workloadChainId);

        this.recheckKeys = recheckKeys;
        this.cacheName = cacheName;
        this.partId = partId;
        this.recheckAttempt = recheckAttempt;
        this.repairAttempt = repairAttempt;
    }

    /**
     * @return Recheck keys.
     */
    public Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys() {
        return recheckKeys;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Recheck attempt.
     */
    public int recheckAttempt() {
        return recheckAttempt;
    }

    /**
     * @return Repair attempt.
     */
    public int repairAttempt() {
        return repairAttempt;
    }
}

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
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;

/**
 * Work container for repair stage.
 */
public class Repair implements PipelineWorkload {
    /**
     * Cache name.
     */
    private String cacheName;

    /**
     * Partition ID.
     */
    private int partId;

    /** Attempt number. */
    private int repairAttempt;

    /** Session id. */
    private long sessionId;

    /** Workload chain id. */
    private final UUID workloadChainId;

    /**
     * Per-node values from recheck phase.
     */
    private Map<KeyCacheObject, Map<UUID, VersionedValue>> data;

    /**
     * @param sessionId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param data Data.
     * @param repairAttempt Repair attempt.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public Repair(
        long sessionId,
        UUID workloadChainId,
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> data,
        int repairAttempt
    ) {
        this.sessionId = sessionId;
        this.workloadChainId = workloadChainId;
        this.cacheName = cacheName;
        this.partId = partId;
        this.data = data;
        this.repairAttempt = repairAttempt;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Partition id.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Data.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<KeyCacheObject, Map<UUID, VersionedValue>> data() {
        return data;
    }

    /**
     * @return Attempt number.
     */
    public int repairAttempt() {
        return repairAttempt;
    }

    /** {@inheritDoc} */
    @Override public long sessionId() {
        return sessionId;
    }

    /** {@inheritDoc} */
    @Override public UUID workloadChainId() {
        return workloadChainId;
    }
}

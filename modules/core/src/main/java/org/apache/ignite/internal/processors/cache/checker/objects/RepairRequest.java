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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;

/**
 * Request object contains a set key for repair.
 */
public class RepairRequest extends CachePartitionRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Keys to repair with corresponding values and versions per node. */
    private Map<KeyCacheObject, Map<UUID, VersionedValue>> data;

    /** Repair algorithm to use while fixing doubtful keys. */
    private RepairAlgorithm repairAlg;

    /** Cache name. */
    private String cacheName;

    /** Partition id. */
    private int partId;

    /** Start topology version. */
    private AffinityTopologyVersion startTopVer;

    /** Repair attempt. */
    private int repairAttempt;

    /**
     * @param sessionId Session id.
     * @param workloadChainId Workload chain id.
     * @param data Data.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param startTopVer Start topology version.
     * @param repairAlg Repair alg.
     * @param repairAttempt Repair attempt.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public RepairRequest(long sessionId, UUID workloadChainId,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> data, String cacheName, int partId,
        AffinityTopologyVersion startTopVer, RepairAlgorithm repairAlg,
        int repairAttempt) {
        super(sessionId, workloadChainId);
        this.data = data;
        this.cacheName = cacheName;
        this.partId = partId;
        this.startTopVer = startTopVer;
        this.repairAlg = repairAlg;
        this.repairAttempt = repairAttempt;
    }

    /**
     * @return Data.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<KeyCacheObject, Map<UUID, VersionedValue>> data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /**
     * @return Repair alg.
     */
    public RepairAlgorithm repairAlg() {
        return repairAlg;
    }

    /**
     * @return Repair attempt.
     */
    public int repairAttempt() {
        return repairAttempt;
    }

    /**
     * @return Start topology version.
     */
    public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }
}

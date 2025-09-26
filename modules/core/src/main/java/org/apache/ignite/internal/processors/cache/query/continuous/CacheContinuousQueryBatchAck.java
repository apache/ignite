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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Batch acknowledgement.
 */
public class CacheContinuousQueryBatchAck extends GridCacheIdMessage {
    /** Routine ID. */
    @Order(4)
    private UUID routineId;

    /** Update counters. */
    @Order(5)
    @GridToStringInclude
    private Map<Integer, Long> updateCntrs;

    /**
     * Default constructor.
     */
    public CacheContinuousQueryBatchAck() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param routineId Routine ID.
     * @param updateCntrs Update counters.
     */
    CacheContinuousQueryBatchAck(int cacheId, UUID routineId, Map<Integer, Long> updateCntrs) {
        this.cacheId = cacheId;
        this.routineId = routineId;
        this.updateCntrs = updateCntrs;
    }

    /**
     * @return Routine ID.
     */
    public UUID routineId() {
        return routineId;
    }

    /**
     * @param routineId Routine ID.
     */
    public void routineId(UUID routineId) {
        this.routineId = routineId;
    }

    /**
     * @return Update counters.
     */
    public Map<Integer, Long> updateCntrs() {
        return updateCntrs;
    }

    /**
     * @param updateCntrs Update counters.
     */
    public void updateCntrs(Map<Integer, Long> updateCntrs) {
        this.updateCntrs = updateCntrs;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 118;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryBatchAck.class, this);
    }
}

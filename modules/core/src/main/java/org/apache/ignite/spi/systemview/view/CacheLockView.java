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

package org.apache.ignite.spi.systemview.view;

import java.util.UUID;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.systemview.Order;
import org.apache.ignite.internal.systemview.SystemViewDescriptor;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Cache key lock representation for a {@link SystemView}.
 * Shows held cache key locks and queued cache key locks hosted on current node,
 * including explicit locks and transactional locks.
 */
@SystemViewDescriptor
public class CacheLockView {
    /** Lock candidate. */
    private final GridCacheMvccCandidate cand;

    /**
     * @param cand Lock candidate
     */
    public CacheLockView(GridCacheMvccCandidate cand) {
        this.cand = cand;
    }

    /**
     * @return Cache ID.
     */
    @Order
    public int cacheId() {
        return cand.key().cacheId();
    }

    /**
     * @return Key.
     */
    @Order(1)
    public String key() {
        return cand.key().key().toString();
    }

    /**
     * @return Node ID.
     */
    @Order(2)
    public UUID nodeId() {
        return cand.nodeId();
    }

    /**
     * @return Originating node ID.
     */
    @Order(3)
    public UUID originatingNodeId() {
        return cand.otherNodeId();
    }

    /**
     * @return "OWNER" flag.
     */
    @Order(4)
    public boolean isOwner() {
        return cand.owner();
    }

    /**
     * @return "TX" flag.
     */
    @Order(5)
    public boolean isTx() {
        return cand.tx();
    }

    /**
     * @return All enabled flags.
     */
    @Order(6)
    public short flags() {
        return cand.flags();
    }

    /**
     * @return Transaction ID.
     */
    @Order(7)
    public IgniteUuid xid() {
        return BinaryUtils.asIgniteUuid(cand.version());
    }

    /**
     * @return Originating transaction ID.
     */
    @Order(8)
    public IgniteUuid originatingXid() {
        return BinaryUtils.asIgniteUuid(cand.otherVersion());
    }
}

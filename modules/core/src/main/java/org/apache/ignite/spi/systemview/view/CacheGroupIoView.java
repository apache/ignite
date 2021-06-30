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

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;

/**
 * Cache group IO statistics representation for a {@link SystemView}.
 */
public class CacheGroupIoView {
    /** Cache group. */
    private final CacheGroupContext grpCtx;

    /** Physical reads. */
    private final long physicalReads;

    /** Logical reads. */
    private final long logicalReads;

    /**
     * @param grpCtx Cache group context.
     * @param physicalReads Physical reads.
     * @param logicalReads Logical reads.
     */
    public CacheGroupIoView(CacheGroupContext grpCtx, long physicalReads, long logicalReads) {
        this.grpCtx = grpCtx;
        this.physicalReads = physicalReads;
        this.logicalReads = logicalReads;
    }

    /**
     * @return Cache group id.
     */
    @Order
    public int cacheGroupId() {
        return grpCtx.groupId();
    }

    /**
     * @return Cache group name.
     */
    @Order(1)
    public String cacheGroupName() {
        return grpCtx.cacheOrGroupName();
    }

    /**
     * @return Physical reads.
     */
    @Order(2)
    public long physicalReads() {
        return physicalReads;
    }

    /**
     * @return Logical reads.
     */
    @Order(3)
    public long logicalReads() {
        return logicalReads;
    }
}

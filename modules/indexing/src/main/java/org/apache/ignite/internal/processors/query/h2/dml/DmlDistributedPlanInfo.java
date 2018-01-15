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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.util.List;

/**
 * Additional information about distributed update plan.
 */
public final class DmlDistributedPlanInfo {
    /** Whether update involves only replicated caches. */
    private final boolean replicatedOnly;

    /** Identifiers of caches involved in update (used for cluster nodes mapping). */
    private final List<Integer> cacheIds;

    /**
     * Constructor.
     *
     * @param replicatedOnly Whether all caches are replicated.
     * @param cacheIds List of cache identifiers.
     */
    public DmlDistributedPlanInfo(boolean replicatedOnly, List<Integer> cacheIds) {
        this.replicatedOnly = replicatedOnly;
        this.cacheIds = cacheIds;
    }

    /**
     * @return {@code true} in case all involved caches are replicated.
     */
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * @return cache identifiers.
     */
    public List<Integer> getCacheIds() {
        return cacheIds;
    }
}

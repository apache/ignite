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

package org.apache.ignite.internal.processors.metric.list.view;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;

/**
 *
 */
public class CacheGroupView implements MonitoringRow<String> {
    /** Cache group. */
    private CacheGroupDescriptor grp;

    private CacheConfiguration<?, ?> ccfg;

    /** Idenity of the user activity causes cache creation. */
    private String sessionId;

    /**
     * @param sessionId Idenity of the user activity causes cache creation.
     * @param grp Cache group.
     */
    public CacheGroupView(String sessionId, CacheGroupDescriptor grp) {
        this.grp = grp;
        this.ccfg = grp.config();
        this.sessionId = sessionId;
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return sessionId;
    }

    /** */
    public int groupId() {
        return grp.groupId();
    }

/*
                        grp.cacheOrGroupName(),
                        grp.sharedGroup(),
                        grp.caches() == null ? 0 : grp.caches().size(),
                    ccfg.getCacheMode(),
                        ccfg.getAtomicityMode(),
    toStringSafe(ccfg.getAffinity()),
        ccfg.getAffinity() != null ? ccfg.getAffinity().partitions() : null,
    nodeFilter(ccfg),
                    ccfg.getDataRegionName(),
    toStringSafe(ccfg.getTopologyValidator()),
        ccfg.getPartitionLossPolicy(),
        ccfg.getRebalanceMode(),
        ccfg.getRebalanceDelay(),
        ccfg.getRebalanceOrder(),
        ccfg.getCacheMode() == CacheMode.REPLICATED ? null : ccfg.getBackups()*/
}

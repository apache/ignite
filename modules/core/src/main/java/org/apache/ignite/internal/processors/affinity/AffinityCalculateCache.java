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

package org.apache.ignite.internal.processors.affinity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class AffinityCalculateCache {
    /** */
    private final Map<Object, List<List<ClusterNode>>> assignCache = new HashMap<>();

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final DiscoveryEvent discoEvt;

    /** */
    private Map<Integer, List<List<ClusterNode>>> grpAssign;

    public AffinityCalculateCache(AffinityTopologyVersion topVer, DiscoveryEvent discoEvt) {
        this.topVer = topVer;
        this.discoEvt = discoEvt;
    }

    public List<List<ClusterNode>> assignPartitions(AffinityFunction aff,
        int backups,
        List<ClusterNode> nodes,
        List<List<ClusterNode>> prevAssignment,
        @Nullable Integer affGrp,
        Object affKey) {
        if (affGrp != null && grpAssign != null) {
            List<List<ClusterNode>> calcAssign = grpAssign.get(affGrp);

            if (calcAssign != null)
                return calcAssign;
        }

        AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(nodes,
            prevAssignment,
            discoEvt,
            topVer,
            backups);

        List<List<ClusterNode>> assign = aff.assignPartitions(ctx);

        List<List<ClusterNode>> assign0 = assignCache.get(affKey);

        if (assign0 != null && assign0.equals(assign))
            assign = assign0;
        else
            assignCache.put(affKey, assign);

        if (affGrp != null) {
            if (grpAssign == null)
                grpAssign = new HashMap<>();

            grpAssign.put(affGrp, assign);
        }

        return assign;
    }
}

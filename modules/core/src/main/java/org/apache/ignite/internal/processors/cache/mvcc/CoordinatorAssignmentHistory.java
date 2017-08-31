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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 *
 */
class CoordinatorAssignmentHistory {
    /** */
    private volatile Map<AffinityTopologyVersion, ClusterNode> assignHist = Collections.emptyMap();

    /** */
    private volatile IgniteBiTuple<AffinityTopologyVersion, ClusterNode>
        cur = new IgniteBiTuple<>(AffinityTopologyVersion.NONE, null);

    void addAssignment(AffinityTopologyVersion topVer, ClusterNode crd) {
        assert !assignHist.containsKey(topVer);
        assert topVer.compareTo(cur.get1()) > 0;

        cur = new IgniteBiTuple<>(topVer, crd);

        Map<AffinityTopologyVersion, ClusterNode> hist = new HashMap<>(assignHist);

        hist.put(topVer, crd);

        assignHist = hist;

    }

    ClusterNode currentCoordinator() {
        return cur.get2();
    }

    ClusterNode coordinator(AffinityTopologyVersion topVer) {
        assert topVer.initialized() : topVer;

        IgniteBiTuple<AffinityTopologyVersion, ClusterNode> cur0 = cur;

        if (cur0.get1().equals(topVer))
            return cur0.get2();

        Map<AffinityTopologyVersion, ClusterNode> assignHist0 = assignHist;

        assert assignHist.containsKey(topVer) :
            "No coordinator assignment [topVer=" + topVer + ", curVer=" + cur0.get1() + ", hist=" + assignHist0.keySet() + ']';

        return assignHist0.get(topVer);
    }
}

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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Topology future created for client cache start.
 */
public class ClientCacheDhtTopologyFuture extends GridDhtTopologyFutureAdapter {
    /** */
    final AffinityTopologyVersion topVer;

    /**
     * @param topVer Topology version.
     */
    public ClientCacheDhtTopologyFuture(AffinityTopologyVersion topVer) {
        assert topVer != null;

        this.topVer = topVer;

        onDone(topVer);
    }

    /**
     * @param topVer Topology version.
     * @param e Error.
     */
    public ClientCacheDhtTopologyFuture(AffinityTopologyVersion topVer, IgniteCheckedException e) {
        assert e != null;
        assert topVer != null;

        this.topVer = topVer;

        onDone(e);
    }

    /**
     * @param grp Cache group.
     * @param topNodes Topology nodes.
     */
    public void validate(CacheGroupContext grp, Collection<ClusterNode> topNodes) {
        grpValidRes = U.newHashMap(1);

        CacheGroupValidation valRes = validateCacheGroup(grp, topNodes);

        if (!valRes.isValid() || valRes.hasLostPartitions())
            grpValidRes.put(grp.groupId(), valRes);
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion initialVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean exchangeDone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean changedAffinity() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ClientCacheDhtTopologyFuture [topVer=" + topVer + ']';
    }
}

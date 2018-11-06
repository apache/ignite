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

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class MvccCoordinator implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID nodeId;

    /**
     * Unique coordinator version, increases when new coordinator is assigned,
     * can differ from topVer if we decide to assign coordinator manually.
     */
    private final long crdVer;

    /** */
    @GridToStringInclude
    private final AffinityTopologyVersion topVer;

    /**
     * @param nodeId Coordinator node ID.
     * @param crdVer Coordinator version.
     * @param topVer Topology version when coordinator was assigned.
     */
    public MvccCoordinator(UUID nodeId, long crdVer, AffinityTopologyVersion topVer) {
        assert nodeId != null;
        assert crdVer > 0 : crdVer;
        assert topVer != null;

        this.nodeId = nodeId;
        this.crdVer = crdVer;
        this.topVer = topVer;
    }

    /**
     * @return Unique coordinator version.
     */
    public long coordinatorVersion() {
        return crdVer;
    }

    /**
     * @return Coordinator node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Topology version when coordinator was assigned.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MvccCoordinator that = (MvccCoordinator)o;

        return crdVer == that.crdVer;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(crdVer ^ (crdVer >>> 32));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccCoordinator.class, this);
    }
}

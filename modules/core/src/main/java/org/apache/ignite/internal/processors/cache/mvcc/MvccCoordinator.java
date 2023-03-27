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
    public static final MvccCoordinator DISCONNECTED_COORDINATOR =
        new MvccCoordinator(AffinityTopologyVersion.NONE, null, 0, false);

    /** */
    public static final MvccCoordinator UNASSIGNED_COORDINATOR =
        new MvccCoordinator(AffinityTopologyVersion.NONE, null, 0, false);

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final AffinityTopologyVersion topVer;

    /** */
    private final UUID nodeId;

    /**
     * Unique coordinator version, increases when new coordinator is assigned,
     * can differ from topVer if we decide to assign coordinator manually.
     */
    private final long ver;

    /** */
    private final boolean local;

    /** */
    private volatile boolean initialized;

    /**
     * @param topVer Topology version when coordinator was assigned.
     * @param nodeId Coordinator node ID.
     * @param ver Coordinator version.
     * @param local {@code True} if the local node is a coordinator.
     */
    public MvccCoordinator(AffinityTopologyVersion topVer, UUID nodeId, long ver, boolean local) {
        this.topVer = topVer;
        this.nodeId = nodeId;
        this.ver = ver;
        this.local = local;
    }

    /**
     * @return Topology version when coordinator was assigned.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Coordinator node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Unique coordinator version.
     */
    public long version() {
        return ver;
    }

    /**
     *
     * @return {@code True} if the coordinator is local.
     */
    public boolean local() {
        return local;
    }

    /**
     *
     * @return {@code True} if the coordinator is disconnected.
     */
    public boolean disconnected() {
        return this == DISCONNECTED_COORDINATOR;
    }

    /**
     *
     * @return {@code True} if the coordinator has not been assigned yet.
     */
    public boolean unassigned() {
        return this == UNASSIGNED_COORDINATOR;
    }

    /**
     *
     * @return {@code True} if the coordinator is initialized.
     */
    public boolean initialized() {
        return initialized;
    }

    /**
     *
     * @param initialized Initialized flag.
     */
    public void initialized(boolean initialized) {
        this.initialized = initialized;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MvccCoordinator that = (MvccCoordinator)o;

        return ver == that.ver;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(ver ^ (ver >>> 32));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccCoordinator.class, this);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.gridgain.grid.cache.affinity.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache affinity function context implementation. Simple bean that holds all required fields.
 */
public class GridCacheAffinityFunctionContextImpl implements GridCacheAffinityFunctionContext {
    /** Topology snapshot. */
    private List<ClusterNode> topSnapshot;

    /** Previous affinity assignment. */
    private List<List<ClusterNode>> prevAssignment;

    /** Discovery event that caused this topology change. */
    private GridDiscoveryEvent discoEvt;

    /** Topology version. */
    private long topVer;

    /** Number of backups to assign. */
    private int backups;

    /**
     * @param topSnapshot Topology snapshot.
     * @param topVer Topology version.
     */
    public GridCacheAffinityFunctionContextImpl(List<ClusterNode> topSnapshot, List<List<ClusterNode>> prevAssignment,
        GridDiscoveryEvent discoEvt, long topVer, int backups) {
        this.topSnapshot = topSnapshot;
        this.prevAssignment = prevAssignment;
        this.discoEvt = discoEvt;
        this.topVer = topVer;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<ClusterNode> previousAssignment(int part) {
        return prevAssignment.get(part);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> currentTopologySnapshot() {
        return topSnapshot;
    }

    /** {@inheritDoc} */
    @Override public long currentTopologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /** {@inheritDoc} */
    @Override public int backups() {
        return backups;
    }
}

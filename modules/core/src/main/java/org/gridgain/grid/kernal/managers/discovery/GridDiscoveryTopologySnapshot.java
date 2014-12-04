/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.discovery;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.util.*;

/**
 * Topology snapshot managed by discovery manager.
 */
public class GridDiscoveryTopologySnapshot {
    /** Topology version. */
    private long topVer;

    /** Topology nodes. */
    @GridToStringInclude
    private Collection<ClusterNode> topNodes;

    /**
     * Creates a topology snapshot with given topology version and topology nodes.
     *
     * @param topVer Topology version.
     * @param topNodes Topology nodes.
     */
    public GridDiscoveryTopologySnapshot(long topVer, Collection<ClusterNode> topNodes) {
        this.topVer = topVer;
        this.topNodes = topNodes;
    }

    /** {@inheritDoc} */
    public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    public Collection<ClusterNode> topologyNodes() {
        return topNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDiscoveryTopologySnapshot.class, this);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Service per-node assignment.
 */
public class GridServiceAssignments implements Serializable, GridCacheInternal {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private final String name;

    /** Cache name. */
    private final String cacheName;

    /** Affinity key. */
    private final Object affKey;

    /** Node ID. */
    private final UUID nodeId;

    /** Service. */
    private final GridService svc;

    /** Node filter. */
    private final GridPredicate<GridNode> nodeFilter;

    /** Topology version. */
    private final long topVer;

    /** Assignments. */
    @GridToStringInclude
    private Map<UUID, Integer> assigns = Collections.emptyMap();

    /**
     * @param name Service name.
     * @param svc Service.
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param nodeId Node ID.
     * @param topVer Topology version.
     * @param nodeFilter Node filter.
     */
    public GridServiceAssignments(String name, GridService svc, String cacheName, Object affKey, UUID nodeId,
        long topVer, GridPredicate<GridNode> nodeFilter) {
        this.name = name;
        this.svc = svc;
        this.cacheName = cacheName;
        this.affKey = affKey;
        this.nodeId = nodeId;
        this.topVer = topVer;
        this.nodeFilter = nodeFilter;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Service.
     */
    public GridService service() {
        return svc;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Affinity key.
     */
    public Object affinityKey() {
        return affKey;
    }

    /**
     * @return Origin node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Node filter.
     */
    public GridPredicate<GridNode> nodeFilter() {
        return nodeFilter;
    }

    /**
     * @return Assignments.
     */
    public Map<UUID, Integer> assigns() {
        return assigns;
    }

    /**
     * @param assigns Assignments.
     */
    public void assigns(Map<UUID, Integer> assigns) {
        this.assigns = assigns;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceAssignments.class, this);
    }
}

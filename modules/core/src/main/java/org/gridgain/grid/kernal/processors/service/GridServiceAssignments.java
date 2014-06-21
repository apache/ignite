// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.service.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Service per-node assignment.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceAssignments implements Serializable {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private final String name;

    private final String cacheName;

    private final Object affKey;

    /** Service. */
    private final GridService svc;

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
     * @param topVer Topology version.
     */
    public GridServiceAssignments(String name, GridService svc, String cacheName, Object affKey, long topVer) {
        this.name = name;
        this.svc = svc;
        this.cacheName = cacheName;
        this.affKey = affKey;
        this.topVer = topVer;
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

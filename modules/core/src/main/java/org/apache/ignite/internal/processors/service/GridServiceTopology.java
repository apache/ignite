package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Service deployment topology.
 * There are two mutually exclusive topology assignment options:
 * <ul>
 *     <li>Use {@link GridServiceTopology#perNode()} to assign specific number of service instances to
 *     specific nodes</li>
 *     <li>Use {@link GridServiceTopology#eachNode()} to assign same number of service instances to all nodes</li>
 * </ul>
 */
public class GridServiceTopology implements Serializable {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private final long ver;

    /** Assignments (node ID -> number of service instances) */
    @GridToStringInclude
    private Map<UUID, Integer> perNode = Collections.emptyMap();

    /** Assignments (number of service instances on each node) */
    @GridToStringInclude
    private int eachNode = 0;

    /**
     * Initializes new instance of {@link GridServiceTopology}
     *
     * @param ver topology major version
     */
    public GridServiceTopology(long ver) {
        this.ver = ver;
    }

    /**
     * @return Topology version.
     */
    public long version() {
        return ver;
    }


    /**
     * @return Assignments (node ID -> number of service instances).
     */
    public Map<UUID, Integer> perNode() {
        return perNode;
    }

    /**
     * Assign specific number of service instances to specific nodes. This will clear {@code eachNode} assignment.
     *
     * @param perNode Assignments (node ID -> number of service instances).
     */
    public void perNode(Map<UUID, Integer> perNode) {
        this.perNode = perNode;

        if (eachNode != 0)
            eachNode = 0;
    }

    /**
     * @return Assignments (number of service instances on each node)
     */
    public int eachNode() {
        return eachNode;
    }

    /**
     * Assign same number of service instances to all nodes. This will clear {@code perNode} assignments.
     *
     * @param eachNode Assignments (number of service instances on each node)
     */
    public void eachNode(int eachNode) {
        A.ensure(eachNode > 0, "eachNode must be positive");

        this.eachNode = eachNode;

        if (perNode != null && perNode.size() > 0)
            perNode.clear();
    }

    /**
     * @return true if there are no deployment mappings; false otherwise.
     */
    public boolean isEmpty() {
        return eachNode == 0 && (perNode == null || perNode.size() == 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceTopology.class, this);
    }
}

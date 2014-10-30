/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.gridgain.grid.*;

import java.io.*;
import java.util.*;

/**
 * Cached affinity calculations.
 */
class GridAffinityAssignment implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private final long topVer;

    /** Collection of calculated affinity nodes. */
    private List<List<GridNode>> assignment;

    /** Map of primary node partitions. */
    private final Map<UUID, Set<Integer>> primary;

    /** Map of backup node partitions. */
    private final Map<UUID, Set<Integer>> backup;

    /**
     * Constructs cached affinity calculations item.
     *
     * @param topVer Topology version.
     */
    GridAffinityAssignment(long topVer) {
        this.topVer = topVer;
        primary = new HashMap<>();
        backup = new HashMap<>();
    }

    /**
     * @param topVer Topology version.
     * @param assignment Assignment.
     */
    GridAffinityAssignment(long topVer, List<List<GridNode>> assignment) {
        this.topVer = topVer;
        this.assignment = assignment;

        primary = new HashMap<>();
        backup = new HashMap<>();

        initPrimaryBackupMaps();
    }

    /**
     * @return Affinity assignment.
     */
    public List<List<GridNode>> assignment() {
        return assignment;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * Get affinity nodes for partition.
     *
     * @param part Partition.
     * @return Affinity nodes.
     */
    public List<GridNode> get(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        return assignment.get(part);
    }

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @return Primary partitions for specified node ID.
     */
    public Set<Integer> primaryPartitions(UUID nodeId) {
        Set<Integer> set = primary.get(nodeId);

        return set == null ? Collections.<Integer>emptySet() : Collections.unmodifiableSet(set);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @return Backup partitions for specified node ID.
     */
    public Set<Integer> backupPartitions(UUID nodeId) {
        Set<Integer> set = backup.get(nodeId);

        return set == null ? Collections.<Integer>emptySet() : Collections.unmodifiableSet(set);
    }

    /**
     * Initializes primary and backup maps.
     */
    private void initPrimaryBackupMaps() {
        // Temporary mirrors with modifiable partition's collections.
        Map<UUID, Set<Integer>> tmpPrm = new HashMap<>();
        Map<UUID, Set<Integer>> tmpBkp = new HashMap<>();

        for (int partsCnt = assignment.size(), p = 0; p < partsCnt; p++) {
            // Use the first node as primary, other - backups.
            Map<UUID, Set<Integer>> tmp = tmpPrm;
            Map<UUID, Set<Integer>> map = primary;

            for (GridNode node : assignment.get(p)) {
                UUID id = node.id();

                Set<Integer> set = tmp.get(id);

                if (set == null) {
                    tmp.put(id, set = new HashSet<>());
                    map.put(id, Collections.unmodifiableSet(set));
                }

                set.add(p);

                // Use the first node as primary, other - backups.
                tmp = tmpBkp;
                map = backup;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(topVer ^ (topVer >>> 32));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return topVer == ((GridAffinityAssignment)o).topVer;
    }
}

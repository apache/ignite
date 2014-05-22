/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import java.io.*;

/**
 * Affinity configuration data.
 */
public class VisorAffinityConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String affinity;
    private final String affinityMapper;
    private final int partitionedBackups;
    private final Integer partitions;
    private final Integer dfltReplicas;
    private final Boolean excludeNeighbors;

    public VisorAffinityConfig(String affinity, String affinityMapper, int partitionedBackups, Integer partitions,
        Integer dfltReplicas, Boolean excludeNeighbors) {
        this.affinity = affinity;
        this.affinityMapper = affinityMapper;
        this.partitionedBackups = partitionedBackups;
        this.partitions = partitions;
        this.dfltReplicas = dfltReplicas;
        this.excludeNeighbors = excludeNeighbors;
    }

    /**
     * @return Affinity.
     */
    public String affinity() {
        return affinity;
    }

    /**
     * @return Affinity mapper.
     */
    public String affinityMapper() {
        return affinityMapper;
    }

    /**
     * @return Partitioned backups.
     */
    public int partitionedBackups() {
        return partitionedBackups;
    }

    /**
     * @return Partitions.
     */
    public Integer partitions() {
        return partitions;
    }

    /**
     * @return Default replicas.
     */
    public Integer defaultReplicas() {
        return dfltReplicas;
    }

    /**
     * @return Exclude neighbors.
     */
    public Boolean excludeNeighbors() {
        return excludeNeighbors;
    }
}

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

    /** Cache affinity. */
    private final String affinity;

    /** Cache affinity mapper. */
    private final String affinityMapper;

    /** Count of key backups. */
    private final int partitionedBackups;

    /** Cache affinity partitions. */
    private final Integer partitions;

    /** Cache partitioned affinity default replicas. */
    private final Integer dfltReplicas;

    /** Cache partitioned affinity exclude neighbors. */
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
     * @return Cache affinity.
     */
    public String affinity() {
        return affinity;
    }

    /**
     * @return Cache affinity mapper.
     */
    public String affinityMapper() {
        return affinityMapper;
    }

    /**
     * @return Count of key backups.
     */
    public int partitionedBackups() {
        return partitionedBackups;
    }

    /**
     * @return Cache affinity partitions.
     */
    public Integer partitions() {
        return partitions;
    }

    /**
     * @return Cache partitioned affinity default replicas.
     */
    public Integer defaultReplicas() {
        return dfltReplicas;
    }

    /**
     * @return Cache partitioned affinity exclude neighbors..
     */
    public Boolean excludeNeighbors() {
        return excludeNeighbors;
    }
}

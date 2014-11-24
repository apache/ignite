/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for affinity configuration properties.
 */
public class VisorCacheAffinityConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache affinity function. */
    private String function;

    /** Cache affinity mapper. */
    private String mapper;

    /** Count of key backups. */
    private int partitionedBackups;

    /** Cache affinity partitions. */
    private Integer partitions;

    /** Cache partitioned affinity default replicas. */
    private Integer dfltReplicas;

    /** Cache partitioned affinity exclude neighbors. */
    private Boolean excludeNeighbors;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for affinity configuration properties.
     */
    public static VisorCacheAffinityConfig from(GridCacheConfiguration ccfg) {
        GridCacheAffinityFunction aff = ccfg.getAffinity();

        Integer dfltReplicas = null;
        Boolean excludeNeighbors = null;

        if (aff instanceof GridCacheConsistentHashAffinityFunction) {
            GridCacheConsistentHashAffinityFunction hashAffFunc = (GridCacheConsistentHashAffinityFunction)aff;

            dfltReplicas = hashAffFunc.getDefaultReplicas();
            excludeNeighbors = hashAffFunc.isExcludeNeighbors();
        }

        VisorCacheAffinityConfig cfg = new VisorCacheAffinityConfig();

        cfg.function(compactClass(aff));
        cfg.mapper(compactClass(ccfg.getAffinityMapper()));
        cfg.partitionedBackups(ccfg.getBackups());
        cfg.defaultReplicas(dfltReplicas);
        cfg.excludeNeighbors(excludeNeighbors);

        return cfg;
    }

    /**
     * @return Cache affinity.
     */
    public String function() {
        return function;
    }

    /**
     * @param function New cache affinity function.
     */
    public void function(String function) {
        this.function = function;
    }

    /**
     * @return Cache affinity mapper.
     */
    public String mapper() {
        return mapper;
    }

    /**
     * @param mapper New cache affinity mapper.
     */
    public void mapper(String mapper) {
        this.mapper = mapper;
    }

    /**
     * @return Count of key backups.
     */
    public int partitionedBackups() {
        return partitionedBackups;
    }

    /**
     * @param partitionedBackups New count of key backups.
     */
    public void partitionedBackups(int partitionedBackups) {
        this.partitionedBackups = partitionedBackups;
    }

    /**
     * @return Cache affinity partitions.
     */
    public Integer partitions() {
        return partitions;
    }

    /**
     * @param partitions New cache affinity partitions.
     */
    public void partitions(Integer partitions) {
        this.partitions = partitions;
    }

    /**
     * @return Cache partitioned affinity default replicas.
     */
    @Nullable public Integer defaultReplicas() {
        return dfltReplicas;
    }

    /**
     * @param dfltReplicas New cache partitioned affinity default replicas.
     */
    public void defaultReplicas(Integer dfltReplicas) {
        this.dfltReplicas = dfltReplicas;
    }

    /**
     * @return Cache partitioned affinity exclude neighbors.
     */
    @Nullable public Boolean excludeNeighbors() {
        return excludeNeighbors;
    }

    /**
     * @param excludeNeighbors New cache partitioned affinity exclude neighbors.
     */
    public void excludeNeighbors(Boolean excludeNeighbors) {
        this.excludeNeighbors = excludeNeighbors;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheAffinityConfig.class, this);
    }
}

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

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for affinity configuration properties.
 */
public class VisorCacheAffinityConfiguration implements Serializable {
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
    public static VisorCacheAffinityConfiguration from(CacheConfiguration ccfg) {
        GridCacheAffinityFunction aff = ccfg.getAffinity();

        Integer dfltReplicas = null;
        Boolean excludeNeighbors = null;

        if (aff instanceof GridCacheConsistentHashAffinityFunction) {
            GridCacheConsistentHashAffinityFunction hashAffFunc = (GridCacheConsistentHashAffinityFunction)aff;

            dfltReplicas = hashAffFunc.getDefaultReplicas();
            excludeNeighbors = hashAffFunc.isExcludeNeighbors();
        }

        VisorCacheAffinityConfiguration cfg = new VisorCacheAffinityConfiguration();

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
        return S.toString(VisorCacheAffinityConfiguration.class, this);
    }
}

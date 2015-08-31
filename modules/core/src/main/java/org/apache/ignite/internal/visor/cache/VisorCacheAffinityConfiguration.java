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

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

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

    /** Cache partitioned affinity exclude neighbors. */
    private Boolean excludeNeighbors;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for affinity configuration properties.
     */
    public static VisorCacheAffinityConfiguration from(CacheConfiguration ccfg) {
        AffinityFunction aff = ccfg.getAffinity();

        Boolean excludeNeighbors = null;

        if (aff instanceof RendezvousAffinityFunction) {
            RendezvousAffinityFunction hashAffFunc = (RendezvousAffinityFunction)aff;

            excludeNeighbors = hashAffFunc.isExcludeNeighbors();
        }

        VisorCacheAffinityConfiguration cfg = new VisorCacheAffinityConfiguration();

        cfg.function = compactClass(aff);
        cfg.mapper = compactClass(ccfg.getAffinityMapper());
        cfg.partitions = aff.partitions();
        cfg.partitionedBackups = ccfg.getBackups();
        cfg.excludeNeighbors = excludeNeighbors;

        return cfg;
    }

    /**
     * @return Cache affinity.
     */
    public String function() {
        return function;
    }

    /**
     * @return Cache affinity mapper.
     */
    public String mapper() {
        return mapper;
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
     * @return Cache partitioned affinity exclude neighbors.
     */
    @Nullable public Boolean excludeNeighbors() {
        return excludeNeighbors;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheAffinityConfiguration.class, this);
    }
}